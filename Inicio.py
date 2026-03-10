import logging
from datetime import datetime
from etl.EtlVisitas import ETLVisitas
from util.FileControl import FileControl
import util.Utilerias as util
import os
logger = logging.getLogger("Inicio")
control = FileControl()

logging.basicConfig(
    level=logging.INFO,
    format='%(levelname)s:%(name)s:%(message)s'
)


# Conexion al Sftp
archivos_pendientes = control.obtener_archivos_en_ruta()

logger.info(f"--------- Iniciando revisión diaria: {datetime.now()} ----------------")
logger.info("Buscando archivos nuevos para procesar")


def verifica_estatus_carga(estatus_etl, archivo_actual):
    if estatus_etl:
        control.registrar_en_bitacora(os.path.basename(archivo_actual))
        control.genera_respaldo(archivo_actual)
        control.elimina_archivo_txt(archivo_actual)
        logger.info(f"[OK] {archivo_actual} registrado en bitácora.")
    else:
        logger.error(f"[Fallo] El archivo {archivo_actual} no se registro en la bitacora")


while len(archivos_pendientes) > 0:
    archivo_actual = archivos_pendientes.pop(0)

    if not control.archivo_ya_procesado(os.path.basename(archivo_actual)):
        logger.info(f"[*] Procesando nuevo archivo: {archivo_actual}")

        if not util.validar_layout(archivo_actual):
            logger.warning(f"[!] Saltando {archivo_actual} por error de layout.")
            continue

        if not util.valida_campos(archivo_actual):
            logger.warning(f"[!] Saltando {archivo_actual} por datos no validos.")
            continue

        etl = ETLVisitas(archivo_actual)
        estatus_etl = etl.procesar()
        verifica_estatus_carga(estatus_etl, archivo_actual)
    else:
        logger.info(f"[X] Saltando {archivo_actual}: ya existe en bitácora.")

logger.info("--- Proceso finalizado ---")



