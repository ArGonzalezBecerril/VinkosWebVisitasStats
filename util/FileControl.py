import os
import zipfile
from datetime import datetime
import logging

logger = logging.getLogger("FileControl")
logging.basicConfig(
    level=logging.INFO,
    format='%(levelname)s:%(name)s:%(message)s'
)


class FileControl:
    def __init__(self):
        self.ruta_base = "/home/usuario/vinkOS/archivosVisitas"
        self.ruta_respaldo = "/home/usuario/vinkOS/respaldos"
        self.bitacora_path = os.path.join(os.getcwd(), "config", "bitacora_control.txt")

    def obtener_archivos_en_ruta(self):
        """Abre la ruta base y retorna la ruta completa de cada archivo report_*.txt"""
        if not os.path.exists(self.ruta_base):
            return []
        return [
            os.path.join(self.ruta_base, f)
            for f in os.listdir(self.ruta_base)
            if f.startswith("report_") and f.endswith(".txt")
        ]

    def archivo_ya_procesado(self, nombre_archivo):
        """Busca el archivo en config/bitacora_control.txt"""
        if not os.path.exists(self.bitacora_path):
            return False
        with open(self.bitacora_path, 'r') as f:
            for linea in f:
                if linea.startswith(nombre_archivo + "|"):
                    return True
        return False

    def registrar_en_bitacora(self, nombre_archivo):
        """nombre|peso|fecha"""
        ruta_completa = os.path.join(self.ruta_base, nombre_archivo)
        peso = os.path.getsize(ruta_completa) / 1024
        fecha = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        with open(self.bitacora_path, 'a') as f:
            f.write(f"{nombre_archivo}|{peso:.2f}KB|{fecha}\n")

    def genera_respaldo(self, ruta_archivo_actual):
        """Zipea el archivo y lo guarda en la ruta de respaldo."""
        nombre_archivo = os.path.basename(ruta_archivo_actual)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        nombre_zip = f"{nombre_archivo}_{timestamp}.zip"
        ruta_zip_final = os.path.join(self.ruta_respaldo, nombre_zip)
        try:
            with zipfile.ZipFile(ruta_zip_final, 'w', zipfile.ZIP_DEFLATED) as zipf:
                zipf.write(ruta_archivo_actual, nombre_archivo)

            logging.info(f"[BACKUP] Archivo respaldado en: {ruta_zip_final}")
            return True
        except Exception as e:
            logging.error(f"[!] Error al generar respaldo de {nombre_archivo}: {e}")
            return False

    def elimina_archivo_txt(self, archivo_txt):
        os.remove(archivo_txt)
        logging.info(f"[OK] ¡Limpieza exitosa! El archivo '{archivo_txt}' fue eliminado de la ruta base.")
