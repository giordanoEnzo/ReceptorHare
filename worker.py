#!/usr/bin/env python3
"""
openclaw_enqueue_worker.py
Redis consumer that forwards tasks to Lapin via `openclaw sessions_send`.
"""
import os
import json
import time
import logging
import signal
import shlex
import subprocess
from contextlib import suppress

import redis
import requests

# --- Configurações via Variáveis de Ambiente ---
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
QUEUE_NAME = os.getenv("QUEUE_NAME", "queue:openclaw_tasks")
ERROR_QUEUE = os.getenv("ERROR_QUEUE", "queue:openclaw_errors")
API_CALLBACK_URL = os.getenv("API_CALLBACK_URL", "https://service-system.hareware.com.br/api/v1/assignments/tasks")
LAPIN_SESSION_KEY = os.getenv("LAPIN_SESSION_KEY", "lapin") # ID do agente no OpenClaw (Ex: lapin)
OPENCLAW_BIN = os.getenv("OPENCLAW_BIN", "openclaw")
MAX_SEND_ATTEMPTS = int(os.getenv("MAX_SEND_ATTEMPTS", "3"))
BACKOFF_BASE = float(os.getenv("BACKOFF_BASE", "2.0"))
CALLBACK_TIMEOUT = float(os.getenv("CALLBACK_TIMEOUT", "10.0")) # Aumentado para segurança da rede
MAX_CALLBACK_RETRIES = int(os.getenv("MAX_CALLBACK_RETRIES", "3"))
CLI_TIMEOUT = int(os.getenv("CLI_TIMEOUT", "120")) # 2 Minutos de margem de segurança

# --- Credenciais de Autenticação para o Callback ---
AUTH_URL = os.getenv("AUTH_URL", "https://service-system.hareware.com.br/api/v1/auth/login")
LAPIN_USERNAME = os.getenv("LAPIN_USERNAME", "lapin@hareware.com.br")
LAPIN_PASSWORD = os.getenv("LAPIN_PASSWORD", "Lapin@2026")

# Logging Setup
logging.basicConfig(
    level=LOG_LEVEL,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
logger = logging.getLogger("HareWare-OpenClaw-Worker")

SHUTDOWN = False

def handle_sigterm(signum, frame):
    global SHUTDOWN
    logger.info("Encadeando desligamento seguro (Signal=%s)...", signum)
    SHUTDOWN = True

signal.signal(signal.SIGINT, handle_sigterm)
signal.signal(signal.SIGTERM, handle_sigterm)

# Sessão HTTP persistente para performance nos callbacks
HTTP = requests.Session()
HTTP.headers.update({"User-Agent": "hareware-worker/1.1"})

def login_to_api():
    """
    Autentica na API e injeta o Token JWT nos headers da sessão.
    """
    try:
        logger.info("Tentando autenticar na API : %s", LAPIN_USERNAME)
        payload = {"username": LAPIN_USERNAME, "password": LAPIN_PASSWORD}
        # Enviando como x-www-form-urlencoded (parâmetro 'data')
        resp = HTTP.post(AUTH_URL, data=payload, timeout=CALLBACK_TIMEOUT)
        
        if 200 <= resp.status_code < 300:
            data = resp.json()
            # Tenta pegar o token por diferentes nomes possíveis na resposta
            token = data.get("access_token") or data.get("token") or data.get("data", {}).get("token")
            
            if token:
                HTTP.headers.update({"Authorization": f"Bearer {token}"})
                logger.info("✅ Login efetuado com sucesso na API HareWare.")
                return True
        logger.error("❌ Falha no login da API: Status %s - %s", resp.status_code, resp.text)
    except Exception as e:
        logger.error("❌ Erro durante a tentativa de login: %s", e)
    return False

def push_error_queue(redis_client: redis.Redis, original_task: dict, error_msg: str):
    try:
        task_id = original_task.get("id", "N/A")
        payload = {
            "original_task": original_task,
            "error": error_msg,
            "failed_at": int(time.time()),
            "worker": "hareware-openclaw-v1"
        }
        redis_client.lpush(ERROR_QUEUE, json.dumps(payload))
        logger.warning("⚠️ Tarefa [%s] movida para a fila de ERROS: %s", task_id, error_msg)
    except Exception:
        logger.exception("🚨 Falha crítica ao empurrar para a fila de erros.")

def update_task_status_api(task_id: str, status: str, log: str) -> bool:
    """
    Atualiza o status na API da HareWare.
    """
    if not task_id:
        return False
    url = f"{API_CALLBACK_URL.rstrip('/')}/{task_id}"
    payload = {"status": status, "description": log}
    
    for attempt in range(1, MAX_CALLBACK_RETRIES + 1):
        try:
            resp = HTTP.put(url, json=payload, timeout=CALLBACK_TIMEOUT)
            if 200 <= resp.status_code < 300:
                logger.info("API HareWare atualizada: Task %s -> %s", task_id, status)
                return True
            logger.warning("API retornou status %s na tentativa %d. Detalhes: %s", resp.status_code, attempt, resp.text)
        except requests.RequestException as e:
            logger.warning("Erro de conexão com a API (Tentativa %d): %s", attempt, e)
        
        time.sleep(BACKOFF_BASE ** (attempt - 1))
    
    logger.error("❌ Falha definitiva ao atualizar API de status para Task %s após %d tentativas.", task_id, MAX_CALLBACK_RETRIES)
    return False

def call_openclaw_sessions_send(message_obj: dict) -> (bool, str):
    """
    Invoca a CLI do OpenClaw para enviar a mensagem à sessão do Lapin.
    """
    json_compact = json.dumps(message_obj, separators=(',', ':'))
    cmd = [OPENCLAW_BIN, "agent", f"--agent={LAPIN_SESSION_KEY}", f"--message={json_compact}"]
    
    try:
        logger.debug("Executando CLI OpenClaw...")
        # Timeout aumentado para 120s conforme solicitado
        proc = subprocess.run(cmd, capture_output=True, text=True, timeout=CLI_TIMEOUT)
        
        combined_output = (proc.stdout or "") + (proc.stderr or "")
        
        if proc.returncode == 0:
            logger.info("Comando 'agent' executado com sucesso.")
            return True, combined_output.strip()
        else:
            logger.error("CLI falhou (Exit Code %s): %s", proc.returncode, combined_output.strip())
            return False, combined_output.strip()
            
    except FileNotFoundError:
        return False, f"Binário '{OPENCLAW_BIN}' não encontrado no sistema."
    except subprocess.TimeoutExpired:
        return False, f"Timeout: CLI OpenClaw excedeu os {CLI_TIMEOUT}s de execução."
    except Exception as e:
        return False, str(e)

def forward_to_webhook(url: str, task_data: dict) -> (bool, str):
    """
    Forwards the task data to a custom webhook URL via HTTP POST.
    """
    try:
        logger.info("Encaminhando tarefa para Webhook: %s", url)
        resp = HTTP.post(url, json=task_data, timeout=CALLBACK_TIMEOUT)
        if 200 <= resp.status_code < 300:
            logger.info("✅ Webhook notificado com sucesso (Status %s).", resp.status_code)
            return True, "Sucesso"
        else:
            logger.error("❌ Webhook retornou erro: Status %s - %s", resp.status_code, resp.text)
            return False, f"HTTP Error {resp.status_code}"
    except Exception as e:
        logger.error("❌ Erro ao enviar para o Webhook: %s", e)
        return False, str(e)

def build_external_event_message(task_data: dict) -> dict:
    """
    Mapeia os dados do seu sistema para o formato esperado pelo Lapin.
    """
    req_id = task_data.get("id") or f"gen-{int(time.time())}"
    
    action_type = task_data.get("action", "created")
    action_label = {
        "created": "Nova task",
        "updated": "Task atualizada",
        "deleted": "Task excluída"
    }.get(action_type, "Task")

    metadata = task_data.get("metadata", {})
    metadata["action_type"] = action_type
    
    # Adicionar novos campos do sistema Hare
    metadata["impact"] = task_data.get("impact")
    metadata["confidence"] = task_data.get("confidence")
    metadata["effort"] = task_data.get("effort")
    metadata["deadline"] = task_data.get("deadline")
    metadata["ticket_info"] = task_data.get("ticket") or task_data.get("ticket_id")
    metadata["ice_score"] = task_data.get("ice_score")
    metadata["status"] = task_data.get("status")

    # Mapeamento de prioridade baseado no ICE Score se o campo priority for nulo
    priority = task_data.get("priority")
    if not priority and task_data.get("ice_score"):
        try:
            score = int(task_data.get("ice_score", 0))
            if score >= 20: priority = "high"
            elif score >= 10: priority = "normal"
            else: priority = "low"
        except (ValueError, TypeError):
            priority = "normal"

    return {
        "type": "external.event",
        "source": "webhook_redis_bridge",
        "payload": {
            "request_id": req_id,
            "created_at": task_data.get("created_at") or time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "action": {
                "id": req_id,
                "title": task_data.get("title", "Tarefa Sem Título"),
                "description": task_data.get("description", ""),
                "priority": str(priority or "normal"),
                "metadata": metadata
            }
        },
        "text": f"{action_label} da HareWare: {task_data.get('title')}"
    }

def process_item(redis_client: redis.Redis, raw_item: str):
    """
    Decodifica, formata e envia a tarefa com retentativas.
    """
    try:
        task_data = json.loads(raw_item)
    except Exception as e:
        logger.error("Mensagem JSON inválida recebida: %s", e)
        push_error_queue(redis_client, {"raw": raw_item}, "invalid_json")
        return

    task_id = task_data.get("id")
    task_title = task_data.get("title", "Sem título")
    logger.info("📦 Processando nova tarefa do Redis: [%s] %s", task_id, task_title)

    notification_url = task_data.get("notification_url")

    # 1. Forward to Webhook if available
    webhook_success = True
    if notification_url:
        webhook_success, webhook_error = forward_to_webhook(notification_url, task_data)
        if not webhook_success:
            logger.warning("Falha ao notificar Webhook: %s", webhook_error)
    
    # 2. Forward to OpenClaw CLI (Original logic)
    message_obj = build_external_event_message(task_data)
    cli_success = False
    cli_output = ""

    for attempt in range(1, MAX_SEND_ATTEMPTS + 1):
        cli_success, cli_output = call_openclaw_sessions_send(message_obj)
        
        if cli_success:
            break
        
        logger.warning("Falha ao enviar para OpenClaw CLI (Tentativa %d/%d)", attempt, MAX_SEND_ATTEMPTS)
        if attempt < MAX_SEND_ATTEMPTS:
            time.sleep(BACKOFF_BASE ** (attempt - 1))

    # Determine final result (at least one must succeed)
    if webhook_success or cli_success:
        # Notifica a API que a tarefa entrou em processamento
        if os.getenv("UPDATE_API_AFTER_ENQUEUE", "true").lower() == "true":
            update_task_status_api(task_id, "Execução", "Tarefa encaminhada com sucesso.")
        logger.info("✅ Tarefa [%s] processada com sucesso.", task_id)
        return

    logger.error("🚨 ERRO CRÍTICO: Tarefa [%s] falhou em todos os destinos (Webhook e CLI).", task_id)
    push_error_queue(redis_client, task_data, f"FAILURE: Webhook({webhook_success}), CLI({cli_success}) - {cli_output}")

def run_loop():
    logger.info("🚀 Worker HareWare iniciado. Monitorando Redis: %s", REDIS_URL)
    
    # Realiza login inicial antes de entrar no loop
    login_to_api()
    
    while not SHUTDOWN:
        try:
            # Recria a conexão se necessário dentro do loop principal
            redis_client = redis.from_url(REDIS_URL, decode_responses=True)
            
            while not SHUTDOWN:
                # Timeout de 5s no blpop para verificar a flag SHUTDOWN periodicamente
                result = redis_client.blpop(QUEUE_NAME, timeout=5)
                
                if result:
                    _, raw_item = result
                    process_item(redis_client, raw_item)
                    
        except redis.ConnectionError as e:
            logger.error("Conexão com Redis perdida: %s. Reconectando em 5s...", e)
            time.sleep(5)
        except Exception as e:
            logger.exception("Erro inesperado no loop principal: %s", e)
            time.sleep(2)
        finally:
            with suppress(Exception):
                redis_client.close()

    logger.info("Worker desligado com sucesso. Até logo!")

if __name__ == "__main__":
    run_loop()
