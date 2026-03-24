#!/usr/bin/env python3
"""
bridge.py
Gateway Flask que recebe Webhooks da HareWare e enfileira no Redis para o worker processar.
"""
import os
import json
import logging
from flask import Flask, request, jsonify
import redis

# --- Configurações via Variáveis de Ambiente ---
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
QUEUE_NAME = os.getenv("QUEUE_NAME", "queue:openclaw_tasks")
BRIDGE_PORT = int(os.getenv("BRIDGE_PORT", "5050")) # Porta 5050 para evitar conflitos no macOS (AirPlay)

# Logging Setup
logging.basicConfig(
    level=LOG_LEVEL,
    format="%(asctime)s [%(levelname)s] HareWare-Webhook-Bridge: %(message)s"
)
logger = logging.getLogger("Bridge")

app = Flask(__name__)

# Estabelecer conexão com Redis
try:
    redis_client = redis.from_url(REDIS_URL, decode_responses=True)
    logger.info("✅ Conectado ao Redis em %s para gerenciar a fila.", REDIS_URL)
except Exception as e:
    logger.error("❌ Falha crítica ao conectar ao Redis: %s", e)
    redis_client = None

@app.route("/webhook", methods=["POST"])
def handle_webhook():
    """
    Endpoint principal para receber alertas ou ações do sistema HareWare.
    """
    if not redis_client:
        return jsonify({"error": "Redis não disponível no momento"}), 503

    try:
        # Pega o payload JSON
        task_data = request.json
        if not task_data:
            return jsonify({"error": "Payload JSON vazio ou inválido"}), 400

        # Log de auditoria simples
        task_title = task_data.get("title", "Sem título")
        task_id = task_data.get("id", "N/A")
        logger.info("Injetando nova tarefa na fila: [%s] %s", task_id, task_title)

        # Enfileira na 'queue:openclaw_tasks' (mesma do worker.py)
        redis_client.lpush(QUEUE_NAME, json.dumps(task_data))
        
        return jsonify({
            "status": "success",
            "message": "Tarefa recebida e enfileirada",
            "queue": QUEUE_NAME
        }), 201

    except Exception as e:
        logger.exception("Erro ao processar requisição de webhook: %s", e)
        return jsonify({"error": str(e)}), 500

@app.route("/health", methods=["GET"])
def health_check():
    """Check de saúde para monitoramento local."""
    return jsonify({"status": "live", "redis_ok": redis_client is not None})

if __name__ == "__main__":
    logger.info("🚀 Gateway de Webhook subindo na porta %s...", BRIDGE_PORT)
    logger.info("URL Final de Integração: http://localhost:%s/webhook", BRIDGE_PORT)
    # 0.0.0.0 permite acesso de outros dispositivos na mesma rede local
    app.run(host="0.0.0.0", port=BRIDGE_PORT)
