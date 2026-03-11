# VinkosWebVisitasStats


## 🚀 Descripción del Proyecto


Este sistema integra información de archivos planos (.txt) alojados en un servidor remoto vía SFTP, encargándose de consolidar el historial de visitantes y métricas de comportamiento (opens, clicks, etc.) en una base de datos MariaDB. El core del proyecto resuelve el manejo de concurrencia de datos mediante lógica de Upsert (Update or Insert) para mantener contadores precisos por usuario

## 🛠️ Stack Tecnológico


- **Lenguaje**: Python 3.x
- **Librerías Core**: Pandas  (Procesamiento de datos), SQLAlchemy (ORM/Conexión BD), Paramiko (SFTP).
- **Base de Datos**: MariaDB / MySQL.
- **Visualización**: Metabase (Dashboarding sobre Java 21).

## ⚙️ Flujo del Proceso ETL
>1. **Extracción (Extract)**
    Conexión SFTP: Conexión al servidor 8.8.8.8 para identificar archivos con patrón report_*.txt.
    Control de Duplicados: Validación contra bitácora local para no procesar el mismo archivo dos veces.
    Descarga y Backup: Los archivos se descargan, se comprimen en .zip en /home/etl/visitas/bckp y se eliminan del origen.

>2. **Transformación (Transform)**
    Validación de Layout: Verificación de columnas requeridas (Email, JV, etc.).
    Limpieza de Datos:
        Normalización de Emails (lowercase/strip).
        Casteo de fechas a formato YYYY-MM-DD HH:mm.
    Lógica de Negocio: Cálculo de visitasAnioActual y visitasMesActual basado en la fecha de ejecución.

>3. **Carga (Load)**
    Tabla visitante: Implementación de UPSERT mediante tabla temporal. Si el email existe, se suma la visita al contador histórico y se actualiza la fechaUltimaVisita.
    Tabla estadistica: Carga masiva de métricas de interacción (Opens/Clicks).
    Tabla errores: Desvío de registros con emails inválidos o fechas mal formadas.


## 🏗️ Puntos de Control y Errores

Logs Detallados: Bitácora en tiempo real con niveles INFO y ERROR.
Manejo de Excepciones: Si la carga falla, la tabla temporal se elimina y se hace un rollback lógico para mantener la integridad.
Notificación: El sistema reporta el estatus final de cada archivo procesado.

📦 Liberación a Producción

**Herramientas/Software**
```sh
# Instalar MariaDB
arturo@debian$ sudo apt update
#Instalar Python3
arturo@debian$ sudo apt install python3-dev libmariadb-dev build-essential -y
```

**Modulos**
```sh
# Instalar MariaDB
arturo@debian$ pip install pandas sqlalchemy mysql-connector-python mariadb python-dotenv paramiko
```


**Ejecutar el código**
```sh
# Situarse en Inicio.py y ejecutar los siguiente
arturo@debian$ /usr/bin/python3.8 /home/usuario/PycharmProjects/VinkVisitEtl/Inicio.py

INFO:Inicio:--------- Iniciando revisión diaria: 2026-03-10 14:50:40.425729 ----------------
INFO:Inicio:Buscando archivos nuevos para procesar
INFO:Inicio:[*] Procesando nuevo archivo: /home/arturo/vinkOS/archivosVisitas/report_7.txt
INFO:ETLVisitas:Iniciando ETL
INFO:root:[Ok] Se cargaran a la BD la informacion de Visitas y Estadisticas
INFO:root:Iniciando carga de Visitas...
INFO:mysql.connector:package: mysql.connector.plugins
INFO:mysql.connector:plugin_name: mysql_native_password
INFO:mysql.connector:AUTHENTICATION_PLUGIN_CLASS: MySQLNativePasswordAuthPlugin
INFO:root:¡Carga a PROD terminada con éxito!
INFO:root:Iniciando carga de Estadísticas...
```
#### Desglose de cada modulo o herramienta


**pandas:** Para el procesamiento y transformación de los DataFrames.
    
**sqlalchemy:** El motor para conectar Python con la base de datos.

**mysql-connector-python:** El driver que pidió tu script (mysql+mysqlconnector).

**mariadb:** El conector nativo por si quieres más velocidad.

**python-dotenv:** Para no dejar tus contraseñas embarradas en el código (usar el .env).

**paramiko:** Necesario para la extracción por SFTP del servidor 8.8.8.8.(Este no se uso ya que por ahora se simulo un folder local)

## Ambientación de BD

Crear las tablas
```sql

create database bd_visitas;
use bd_visitas;
--Estadistica
DROP TABLE IF EXISTS `estadistica`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `estadistica` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `email` varchar(150) DEFAULT NULL,
  `jv` varchar(50) DEFAULT NULL,
  `Badmail` varchar(10) DEFAULT 'No',
  `Baja` varchar(10) DEFAULT 'No',
  `Fecha_envio` datetime DEFAULT NULL,
  `Fecha_open` datetime DEFAULT NULL,
  `Opens` int(11) DEFAULT NULL,
  `Opens_virales` int(11) DEFAULT NULL,
  `Fecha_click` datetime DEFAULT NULL,
  `Clicks` int(11) DEFAULT NULL,
  `Clicks_virales` int(11) DEFAULT NULL,
  `Links` text DEFAULT NULL,
  `IPs` text DEFAULT NULL,
  `Navegadores` text DEFAULT NULL,
  `Plataformas` text DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=504 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--Visitante
DROP TABLE IF EXISTS `visitante`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `visitante` (
  `email` varchar(255) NOT NULL,
  `fechaPrimeraVisita` datetime DEFAULT NULL,
  `fechaUltimaVisita` datetime DEFAULT NULL,
  `visitasTotales` bigint(20) DEFAULT NULL,
  `visitasAnioActual` bigint(20) DEFAULT NULL,
  `visitasMesActual` bigint(20) DEFAULT NULL,
  PRIMARY KEY (`email`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;
/*!40101 SET character_set_client = @saved_cs_client */;
/*!40103 SET TIME_ZONE=@OLD_TIME_ZONE */;

```
