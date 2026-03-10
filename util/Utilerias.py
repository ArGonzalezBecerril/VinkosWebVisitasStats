import csv
import logging
import re
from datetime import datetime

logger = logging.getLogger("Utilerias")

logging.basicConfig(
    level=logging.INFO,
    format='%(levelname)s:%(name)s:%(message)s'
)


def validar_layout_bkp(ruta_archivo):
    """Valida el layout de los archivos que encontro en la ruta base"""
    LAYOUT_MAESTRO = [
        "email", "jk", "Badmail", "Baja", "Fecha envio", "Fecha open",
        "Opens", "Opens virales", "Fecha click", "Clicks",
        "Clicks virales", "Links", "IPs", "Navegadores", "Plataformas"
    ]

    try:
        with open(ruta_archivo, 'r', encoding='utf-8') as f:
            reader = csv.reader(f)
            try:
                header_real = next(reader)
            except StopIteration:
                logger.info(f"[!] Archivo vacío: {ruta_archivo}")
                return False
            header_real = [col.strip() for col in header_real]

            if header_real == LAYOUT_MAESTRO:
                return True
            else:
                # Localizamos exactamente cuál es la columna que no viene correctamente
                for i, (real, esperado) in enumerate(zip(header_real, LAYOUT_MAESTRO)):
                    if real != esperado:
                        logger.info(
                            f"[!] Error en {ruta_archivo}: Columna {i} ('{real}') no coincide con el layout maestro ('{esperado}')")

                # Si la longitud es diferente, también avisamos
                if len(header_real) != len(LAYOUT_MAESTRO):
                    logger.info(f"[!] El número de columnas no coincide: {len(header_real)} vs {len(LAYOUT_MAESTRO)}")
                return False

    except Exception as e:
        logger.info(f"[!] Error al abrir el archivo {ruta_archivo}: {e}")
        return False


def validar_layout(ruta_archivo):
    LAYOUT_MAESTRO = [
        "email", "jk", "Badmail", "Baja", "Fecha envio", "Fecha open",
        "Opens", "Opens virales", "Fecha click", "Clicks",
        "Clicks virales", "Links", "IPs", "Navegadores", "Plataformas"
    ]
    try:
        with open(ruta_archivo, 'r', encoding='utf-8') as f:
            reader = csv.reader(f)
            try: header_real = [col.strip() for col in next(reader)]
            except StopIteration:
                return False

            if header_real == LAYOUT_MAESTRO:
                return True
            errores = 0
            for i, esperado in enumerate(LAYOUT_MAESTRO):
                real = header_real[i] if i < len(header_real) else "NADA"
                if real != esperado:
                    errores += 1
                    logger.info(f"[!] Error en {ruta_archivo}: Columna {i} ('{real}') != '{esperado}'")

                if errores >= 3:
                    logger.error(f"[!!!] El layout de '{ruta_archivo}' no coincide con el layout base, revisalo.")
                    return False
            return False if errores > 0 else True

    except Exception as e:
        logger.info(f"[!] Error: {e}")
        return False


def valida_campos_bkp(ruta_completa):
    """Recorre el archivo y valida que TODOS los emails y fechas
       cumplan el formato. Si falla uno, falla todo."""

    patron_email = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
    formato_fecha = '%d/%m/%Y %H:%M'

    try:
        with open(ruta_completa, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for i, fila in enumerate(reader, start=2):
                email = fila.get('email', '').strip()
                fecha = fila.get('Fecha envio', '').strip()

                # Validación de Email
                if not re.match(patron_email, email):
                    logger.info(f"[!] Email inválido en fila {i}: {email}")
                    return False
                # Validación de Fecha
                try:
                    datetime.strptime(fecha, formato_fecha)
                except ValueError:
                    logger.info(f"[!] Fecha inválida en fila {i}: {fecha}")
                    return False

            return True
    except Exception as e:
        logger.info(f"Error al abrir el archivo para validar campos: {e}")
        return False


def valida_campos(ruta_completa):
    patron_email = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
    formato_fecha = '%d/%m/%Y %H:%M'

    # Define aquí el layout maestro que esperas
    layout_maestro = ['email','jk','Badmail','Baja','Fecha envio','Fecha open','Opens',
                      'Opens virales','Fecha click','Clicks','Clicks virales','Links','IPs','Navegadores','Plataformas'
]
    try:
        with open(ruta_completa, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            columnas_archivo = reader.fieldnames

            total_errores_layout = 0
            for i, columna_esperada in enumerate(layout_maestro):

                if i >= len(columnas_archivo) or columnas_archivo[i] != columna_esperada:
                    logger.info(f"[!] Error de Layout: La columna {i} debería ser '{columna_esperada}'"
                                f" pero llegó '{columnas_archivo[i] if i < len(columnas_archivo) else 'NADA'}'")
                    return False  # Aquí corta el proceso de inmediato

            # --- VALIDACIÓN DE DATOS (SOLO SI EL LAYOUT PASÓ) ---
            for i, fila in enumerate(reader, start=2):
                email = fila.get('email', '').strip()
                fecha = fila.get('Fecha envio', '').strip()

                if not re.match(patron_email, email):
                    logger.info(f"[!] Email inválido en fila {i}: {email}")
                    return False

                try:
                    datetime.strptime(fecha, formato_fecha)
                except ValueError:
                    logger.info(f"[!] Fecha inválida en fila {i}: {fecha}")
                    return False

            return True
    except Exception as e:
        logger.info(f"Error al abrir el archivo para validar campos: {e}")
        return False


def es_layout_valido(total_errores, ruta_completa):
    if total_errores >= 3:
        logger.error(f"[!!!] El layout de '{ruta_completa}' es completamente diferente. Abortando este archivo.")
        return False


def obt_col_visitante():
    return {
        "email": "email",
        "Fecha envio": "Fecha envio"
    }



def obt_col_estadistica():
    pass


