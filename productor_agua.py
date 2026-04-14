#!/usr/bin/env python3
# productor_agua.py - Simula envío de datos de calidad del agua a Kafka

import time
import json
import random
from kafka import KafkaProducer

departamentos = ["CALDAS", "HUILA", "VALLE", "ANTIOQUIA", "BOGOTA", "CUNDINAMARCA", "SANTANDER"]
municipios = {"CALDAS": "AGUADAS", "HUILA": "NEIVA", "VALLE": "CALI", "ANTIOQUIA": "MEDELLIN", "BOGOTA": "BOGOTA", "CUNDINAMARCA": "BOGOTA", "SANTANDER": "BUCARAMANGA"}

propiedades = ["MERCURIO TOTAL EN SEDIMENTOS", "PLOMO TOTAL EN AGUA", "CADMIO TOTAL EN AGUA", "COLIFORMES TOTALES", "pH", "TURBIDEZ"]
unidades = {"MERCURIO TOTAL EN SEDIMENTOS": "mg Hg/kg", "PLOMO TOTAL EN AGUA": "mg Pb/L", "CADMIO TOTAL EN AGUA": "mg Cd/L", "COLIFORMES TOTALES": "UFC/100 mL", "pH": "unidades", "TURBIDEZ": "NTU"}
rangos = {"MERCURIO TOTAL EN SEDIMENTOS": (0.01, 0.5), "PLOMO TOTAL EN AGUA": (0.01, 0.5), "CADMIO TOTAL EN AGUA": (0.001, 0.05), "COLIFORMES TOTALES": (100, 50000), "pH": (6.5, 8.5), "TURBIDEZ": (5, 500)}

def generar_medicion():
    depto = random.choice(departamentos)
    prop = random.choice(propiedades)
    rmin, rmax = rangos.get(prop, (0, 100))
    valor = round(random.uniform(rmin, rmax), 3)
    if random.random() < 0.1:
        valor = f"<{round(rmin, 2)}"
    return {"departamento": depto, "municipio": municipios.get(depto, "DESCONOCIDO"), "propiedad_observada": prop, "resultado": str(valor), "unidad": unidades.get(prop, "mg/L"), "fecha": time.strftime("%Y-%m-%d"), "timestamp": int(time.time()), "fuente": "IDEAM_Simulado"}

producer = KafkaProducer(bootstrap_servers=["localhost:9092"], value_serializer=lambda x: json.dumps(x, ensure_ascii=False).encode("utf-8"))

print("="*60)
print("🌊 PRODUCTOR DE CALIDAD DEL AGUA - IDEAM SIMULADO")
print("📤 Enviando mediciones a Kafka cada 3 segundos")
print("="*60)

try:
    while True:
        medicion = generar_medicion()
        producer.send("agua_calidad", value=medicion)
        print(f"📊 [{medicion['departamento']}] {medicion['propiedad_observada']}: {medicion['resultado']} {medicion['unidad']}")
        time.sleep(3)
except KeyboardInterrupt:
    print("\n🛑 Productor detenido.")
finally:
    producer.close()