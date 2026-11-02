# Evidencia - Pre-entrega 3

Plataforma de muestreo urbano: productor → Kafka → Spark, corriendo en Kubernetes (kind).

## 1. Kafka corriendo en el namespace `urban-sensors`

```bash
kubectl get pods -n urban-sensors
```

```
NAME                     READY   STATUS    RESTARTS   AGE
kafka-659576b668-rkjtj   1/1     Running   0          33s
```

## 2. El Service encontró el pod de Kafka

```bash
kubectl get endpoints kafka -n urban-sensors
```

```
NAME    ENDPOINTS         AGE
kafka   10.244.0.5:9092   3m21s
```

## 3. Tópico `urban_sensors` con 3 particiones

Creación:

```bash
kubectl exec -n urban-sensors deployment/kafka -- \
  /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 \
  --create --topic urban_sensors --partitions 3 --replication-factor 1
```

```
Created topic urban_sensors.
```

Verificación:

```bash
kubectl exec -n urban-sensors deployment/kafka -- \
  /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 \
  --describe --topic urban_sensors
```

```
Topic: urban_sensors    TopicId: Z0kt4uZpQV2OWaLaNvr51g PartitionCount: 3       ReplicationFactor: 1
        Topic: urban_sensors    Partition: 0    Leader: 1       Replicas: 1     Isr: 1
        Topic: urban_sensors    Partition: 1    Leader: 1       Replicas: 1     Isr: 1
        Topic: urban_sensors    Partition: 2    Leader: 1       Replicas: 1     Isr: 1
```

## 4. Productor

El productor corre como un pod (Deployment `producer`) y manda una medición por segundo.

Logs del productor:

```bash
kubectl logs -n urban-sensors deployment/producer --tail=5
```

```
Enviado a urban_sensors: {'sensor_id': 'sensor-03', 'temperature': 20.6, 'humidity': 68.7, 'air_quality_index': 192, 'timestamp': '2026-10-29T20:35:28.123932+00:00'}
Enviado a urban_sensors: {'sensor_id': 'sensor-02', 'temperature': 32.0, 'humidity': 89.7, 'air_quality_index': 296, 'timestamp': '2026-10-29T20:35:29.125202+00:00'}
Enviado a urban_sensors: {'sensor_id': 'sensor-02', 'temperature': 14.8, 'humidity': 69.0, 'air_quality_index': 85, 'timestamp': '2026-10-29T20:35:30.126401+00:00'}
Enviado a urban_sensors: {'sensor_id': 'sensor-03', 'temperature': 31.0, 'humidity': 60.2, 'air_quality_index': 103, 'timestamp': '2026-10-29T20:35:31.126898+00:00'}
Enviado a urban_sensors: {'sensor_id': 'sensor-05', 'temperature': 32.4, 'humidity': 33.3, 'air_quality_index': 36, 'timestamp': '2026-10-29T20:35:32.127914+00:00'}
```

Los mensajes llegan a Kafka (leídos directo del tópico con el consumer de consola):

```bash
kubectl exec -n urban-sensors deployment/kafka -- \
  /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka:9092 \
  --topic urban_sensors --from-beginning --max-messages 5
```

```
{"sensor_id": "sensor-02", "temperature": 14.3, "humidity": 49.7, "air_quality_index": 241, "timestamp": "2026-10-29T20:34:03.827907+00:00"}
{"sensor_id": "sensor-02", "temperature": 13.3, "humidity": 76.9, "air_quality_index": 177, "timestamp": "2026-10-29T20:34:07.667445+00:00"}
{"sensor_id": "sensor-01", "temperature": 27.6, "humidity": 65.2, "air_quality_index": 124, "timestamp": "2026-10-29T20:34:08.667858+00:00"}
{"sensor_id": "sensor-02", "temperature": 19.4, "humidity": 51.1, "air_quality_index": 188, "timestamp": "2026-10-29T20:34:09.669325+00:00"}
{"sensor_id": "sensor-01", "temperature": 18.5, "humidity": 31.0, "air_quality_index": 189, "timestamp": "2026-10-29T20:34:13.672470+00:00"}
Processed a total of 5 messages
```

## 5. Estado del entorno

Pods:

```bash
kubectl get pods -n urban-sensors
```

```
NAME                        READY   STATUS    RESTARTS       AGE
kafka-659576b668-rkjtj      1/1     Running   2 (106m ago)   47h
producer-5986589566-zzjzw   1/1     Running   0              3m10s
```

Todos los recursos del namespace:

```bash
kubectl get all -n urban-sensors
```

```
NAME                            READY   STATUS    RESTARTS       AGE
pod/kafka-659576b668-rkjtj      1/1     Running   2 (106m ago)   47h
pod/producer-5986589566-zzjzw   1/1     Running   0              3m16s

NAME            TYPE        CLUSTER-IP      EXTERNAL-IP   PORT(S)    AGE
service/kafka   ClusterIP   10.96.156.161   <none>        9092/TCP   47h

NAME                       READY   UP-TO-DATE   AVAILABLE   AGE
deployment.apps/kafka      1/1     1            1           47h
deployment.apps/producer   1/1     1            1           3m16s

NAME                                  DESIRED   CURRENT   READY   AGE
replicaset.apps/kafka-659576b668      1         1         1       47h
replicaset.apps/producer-5986589566   1         1         1       3m16s
```

ConfigMaps:

```bash
kubectl get configmap -n urban-sensors
```

```
NAME               DATA   AGE
kafka-config       9      47h
kube-root-ca.crt   1      2d
urban-config       2      47h
```

## 6. Spark procesando

El job de Spark (Deployment `spark-streaming`) lee del tópico `urban_sensors`, arma ventanas de 1 minuto y calcula el promedio de temperatura y de calidad del aire por `sensor_id`.

```bash
kubectl get pods -n urban-sensors
```

```
NAME                              READY   STATUS    RESTARTS      AGE
kafka-659576b668-rkjtj            1/1     Running   3 (75m ago)   5d12h
producer-5986589566-zzjzw         1/1     Running   2 (74m ago)   3d13h
spark-streaming-f9db9f87b-mp7q7   1/1     Running   0             77s
```

Resultados de Spark:

```bash
kubectl logs -n urban-sensors deployment/spark-streaming | grep -E -A12 "Batch:"
```

```
Batch: 0
-------------------------------------------
+------+---------+---------------+---------------------+
|window|sensor_id|avg_temperature|avg_air_quality_index|
+------+---------+---------------+---------------------+
+------+---------+---------------+---------------------+

-------------------------------------------
Batch: 1
-------------------------------------------
+------------------------------------------+---------+------------------+---------------------+
|window                                    |sensor_id|avg_temperature   |avg_air_quality_index|
+------------------------------------------+---------+------------------+---------------------+
|{2026-11-02 09:46:00, 2026-11-02 09:47:00}|sensor-03|16.633333333333333|123.0                |
|{2026-11-02 09:46:00, 2026-11-02 09:47:00}|sensor-01|24.65             |235.0                |
|{2026-11-02 09:46:00, 2026-11-02 09:47:00}|sensor-04|33.0              |284.0                |
|{2026-11-02 09:46:00, 2026-11-02 09:47:00}|sensor-05|21.16             |143.8                |
|{2026-11-02 09:46:00, 2026-11-02 09:47:00}|sensor-02|28.0              |148.66666666666666   |
+------------------------------------------+---------+------------------+---------------------+

-------------------------------------------
Batch: 2
-------------------------------------------
+------------------------------------------+---------+------------------+---------------------+
|window                                    |sensor_id|avg_temperature   |avg_air_quality_index|
+------------------------------------------+---------+------------------+---------------------+
|{2026-11-02 09:46:00, 2026-11-02 09:47:00}|sensor-03|19.72             |119.8                |
|{2026-11-02 09:46:00, 2026-11-02 09:47:00}|sensor-01|22.75             |198.5                |
|{2026-11-02 09:46:00, 2026-11-02 09:47:00}|sensor-04|25.55             |170.75               |
|{2026-11-02 09:46:00, 2026-11-02 09:47:00}|sensor-05|21.233333333333334|132.33333333333334   |
|{2026-11-02 09:46:00, 2026-11-02 09:47:00}|sensor-02|25.76             |151.8                |
+------------------------------------------+---------+------------------+---------------------+
```

Los promedios de la ventana `09:46:00 - 09:47:00` se van actualizando entre el lote 1 y el 2 a medida que llegan más mensajes del productor (por ejemplo, `sensor-04` baja de 33.0 a 25.55 de temperatura). Eso confirma que Spark está procesando en tiempo real lo que manda el productor a través de Kafka.

El aviso `Current batch is falling behind` (el lote tardó más de los 10 segundos del trigger) es esperable porque todo corre en una PC con pocos recursos; no afecta los resultados.

### Nota sobre un error que tuve

El primer intento de Spark falló con `UnknownTopicOrPartitionException` porque en `spark-config` había escrito el tópico como `urban-sensors` (guion medio) en vez de `urban_sensors` (guion bajo). Lo corregí en el ConfigMap y recreé el pod.