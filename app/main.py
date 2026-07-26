import json
import logging
import time
import os
import paho.mqtt.client as mqtt
from influxdb_client import InfluxDBClient, Point, WriteOptions
from influxdb_client.client.write_api import SYNCHRONOUS

# ---------------------------------------------------------------------------
# CONFIGURACIÓN DE PARÁMETROS
# ---------------------------------------------------------------------------
MQTT_BROKER = os.environ.get("MQTT_BROKER", "localhost")  # O la IP de tu broker EMQX
MQTT_PORT = int(os.environ.get("MQTT_PORT", 1883))  # Puerto por defecto de EMQX
MQTT_DATA_TOPIC = os.environ.get("MQTT_DATA_TOPIC", "clio/v1/sensor_data/+")  # Wildcard para capturar cualquier ID de dispositivo
MQTT_INCIDENT_TOPIC = os.environ.get("MQTT_INCIDENT_TOPIC", "clio/v1/incident/+")
MQTT_CONN_TOPIC = os.environ.get("MQTT_CONN_TOPIC", "clio/v1/connection/+")
MQTT_USERNAME = os.environ.get("MQTT_USERNAME", "influxdb_client")
MQTT_PASSWORD = os.environ.get("MQTT_PASSWORD", "mqtt_password")
MQTT_CLIENT_ID = "influxdb_" + str(int(time.time()))  # ID único basado en timestamp

INFLUX_URL = os.environ.get("INFLUX_URL", "localhost:8086")
INFLUX_TOKEN = os.environ.get("INFLUX_TOKEN", "my-influxdb-token")
INFLUX_ORG = os.environ.get("INFLUX_ORG", "my-org")
INFLUX_BUCKET = os.environ.get("INFLUX_BUCKET", "my-bucket")

# Configuración de Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

# ---------------------------------------------------------------------------
# INICIALIZACIÓN DE CLIENTES
# ---------------------------------------------------------------------------
# 1. Cliente InfluxDB
influx_client = InfluxDBClient(
    url=INFLUX_URL,
    token=INFLUX_TOKEN,
    org=INFLUX_ORG
)

# Configuración de Batching Nativo:
# Envía datos cuando acumule 500 puntos O transcurran 1000 ms (1 segundo)
write_options = WriteOptions(
    batch_size=500,
    flush_interval=1_000, # 1 segundo
    jitter_interval=2_000,  # 2 segundos de jitter para evitar picos de escritura
    retry_interval=5_000, # 5 segundos de espera antes de reintentar
    max_retries=5, # Número máximo de reintentos antes de descartar el batch
    max_retry_delay=30_000,  # 30 segundos
    exponential_base=2
)

write_api = influx_client.write_api(write_options=write_options)

# ---------------------------------------------------------------------------
# CALLBACKS DE MQTT
# ---------------------------------------------------------------------------
def on_connect(client, userdata, flags, rc, properties=None):
    if rc == 0:
        logging.info("Conectado exitosamente al broker EMQX.")
        client.subscribe([(MQTT_DATA_TOPIC, 0), (MQTT_INCIDENT_TOPIC, 0), (MQTT_CONN_TOPIC, 0)])
        logging.info(f"Subscrito a los tópicos: {MQTT_DATA_TOPIC}, {MQTT_INCIDENT_TOPIC}, {MQTT_CONN_TOPIC}")
    else:
        logging.error(f"Error al conectar al broker MQTT. Código de retorno: {rc}")

def on_message(client, userdata, msg):
    try:
        # Asumiendo un tópico tipo: clio/v1/sensor_data/ESP32_01
        topic_parts = msg.topic.split('/')
        resource = topic_parts[2] if len(topic_parts) >= 3 else "unknown"
        device_id = topic_parts[3] if len(topic_parts) >= 4 else "unknown"

        # Parsear el payload JSON
        payload = json.loads(msg.payload.decode('utf-8'))
        payload_variable = payload.get("variable", "unknown")
        payload_value = payload.get("value", None)
        payload_metadata = payload.get("metadata", {})
        timestamp = payload_metadata.get("time", int(time.time()))

        # -------------------------------------------------------------------
        # TRANSFORMACIÓN A INFLUX LINE PROTOCOL
        # -------------------------------------------------------------------
        
        if resource == "connection":
            # Manejar eventos de conexión/desconexión
            point = Point("hub_connection") \
                .tag("device_id", device_id) \
                .field("status", payload_value) \
                .field("retained_at", timestamp) \
                .field("mqttconn_err", payload_metadata.get("mqttconn_err", 0))

            write_api.write(bucket=INFLUX_BUCKET, record=point)
            logging.info(f"Evento de conexión registrado para {device_id}: {payload_value}")
            return  # No procesar más para eventos de conexión

        if resource == "incident":
            # Manejar eventos de incidentes
            point = Point("hub_incident") \
                .tag("device_id", device_id) \
                .field("fault_state", payload_value) \
                .field("ctrl_alarm_code", payload_metadata.get("controller_ac", 0)) \
                .field("moni_alarm_code", payload_metadata.get("monitor_ac", 0)) \
                .field("tmbf_ms", payload_metadata.get("tmbf_ms", 0)) \
                .field("recovery_attempts", payload_metadata.get("recov_attempts", 0)) \
                .field("attempts_left", payload_metadata.get("attempts_left", 0)) \
                .field("ms_since_first", payload_metadata.get("ms_since_first", 0)) \

            write_api.write(bucket=INFLUX_BUCKET, record=point)
            logging.info(f"Incidente registrado para {device_id}: {payload_value}")
            return  # No procesar más para eventos de incidentes

        if resource == "sensor_data" and payload_variable == "ac_hub":
            # Manejar datos de telemetría
            hub_data = payload_metadata.get("hub", [0])
            controller_data = payload_metadata.get("ctrl", [0])
            point = Point("hub_telemetry") \
                .tag("device_id", device_id) \
                .field("system_state", payload_value) \
                .field("system_fault", hub_data[0]) \
                .field("room_temp", hub_data[7]) \
                .field("presence_rate", hub_data[9]) \
                .field("active_setpoint", hub_data[5]) \
                .field("hub_data", json.dumps(hub_data))  # Guardar el array completo como JSON

            if controller_data[0] == 1:  # Solo si el controlador está activo
                point.field("supply_temp", controller_data[1])
                point.field("return_temp", controller_data[2])
                point.field("compressor_relay", controller_data[3])
                point.field("fan_relay", controller_data[4])
                point.field("drain_switch", controller_data[5])
                point.field("sec_since_last_cr", controller_data[6])
                point.field("controller_data", json.dumps(controller_data))  # Guardar el array completo como JSON

            # Se escribe al buffer en memoria (no bloquea el loop de MQTT)
            write_api.write(bucket=INFLUX_BUCKET, record=point)
            logging.info(f"Datos escritos en InfluxDB para {device_id}")

            return

        if resource == "sensor_data" and payload_variable == "health_update":
            # Manejar actualizaciones de salud del sistema
            point = Point("hub_health") \
                .tag("device_id", device_id) \
                .field("free_heap", payload_value) \
                .field("ssid", payload_metadata.get("ssid", "unknown")) \
                .field("rssi", payload_metadata.get("rssi", 0)) \
                .field("wifi_channel", payload_metadata.get("channel", 0)) \
                .field("local_ip", payload_metadata.get("local_ip", "unknown")) \
                .field("last_ntp_update", payload_metadata.get("last_ntp_update", 0)) \
                .field("cpu_temp", payload_metadata.get("cpu_temp", 0)) \
                .field("free_heap", payload_metadata.get("free_heap", 0)) \
                .field("fs_usage", payload_metadata.get("fs_usage", 0)) \
                .field("last_reset_reason", payload_metadata.get("reset_reason", 0)) \

            write_api.write(bucket=INFLUX_BUCKET, record=point)
            logging.info(f"Actualización de salud registrada para {device_id}: {payload_value}")

            return

    except json.JSONDecodeError:
        logging.warning(f"Payload inválido recibido en {msg.topic}: {msg.payload}")
    except Exception as e:
        logging.error(f"Error procesando mensaje: {e}")

# ---------------------------------------------------------------------------
# BUCLE PRINCIPAL (MAIN)
# ---------------------------------------------------------------------------
def main():
    mqtt_client = mqtt.Client(
        callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
        client_id=MQTT_CLIENT_ID,
        clean_session=True,
        protocol=mqtt.MQTTv311)
    mqtt_client.on_connect = on_connect
    mqtt_client.on_message = on_message

    # Reconexión automática habilitada
    mqtt_client.reconnect_delay_set(min_delay=1, max_delay=120)

    ## Conexión segura con TLS (si tu broker lo requiere)
    mqtt_client.tls_set()  # Usa certificados del sistema por defecto

    #username y password si tu broker requiere autenticación
    mqtt_client.username_pw_set(username=MQTT_USERNAME, password=MQTT_PASSWORD)

    try:
        logging.info("Iniciando servicio de ingesta...")
        mqtt_client.connect(MQTT_BROKER, MQTT_PORT, keepalive=60)
        mqtt_client.loop_forever()  # Maneja reconexiones y lecturas en red
        
    except KeyboardInterrupt:
        logging.info("Deteniendo el servicio por solicitud del usuario...")
    finally:
        # Cierre limpio para asegurar que los últimos datos en buffer se envíen
        mqtt_client.disconnect()
        write_api.close()
        influx_client.close()
        logging.info("Servicio detenido de forma segura. Buffer liberado.")

if __name__ == "__main__":
    main()