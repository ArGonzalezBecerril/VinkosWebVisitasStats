import logging
import pandas as pd
import util.Utilerias as util

from util.Database import WebVinkosDao

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(name)s.%(funcName)s | %(message)s')


class ETLVisitas:
    def __init__(self, archivo_txt):
        self.archivo_txt = archivo_txt
        self.logger = logging.getLogger(self.__class__.__name__)
        self.logger.info("Iniciando ETL")
        self.dfrm_datos = None
        self.dfrm_visitantes = None
        self.dfrm_estadisticas = None

    def extraer(self):
        self.dfrm_datos = pd.read_csv(self.archivo_txt)

    def transformar(self):
        df_filtrado = self.dfrm_datos[list(util.obt_col_visitante().values())].copy()
        self.dfrm_visitantes = self.obt_visitantes(df_filtrado)

        #self.dfrm_estadisticas = self.dfrm_datos[util.obt_col_estadistica()]

    def cargar(self):
        dfrm_visitas = self.calculo_visitantes(self.dfrm_visitantes)
        dao_vinkos = WebVinkosDao()
        estatus_carga_visitas = dao_vinkos.inserta_visitas(dfrm_visitas)
        estatus_estadistica = dao_vinkos.inserta_estadistica(self.dfrm_datos)

        return estatus_estadistica and estatus_carga_visitas

    def procesar(self):
        self.extraer()
        self.transformar()
        resultado_etl = self.valida_carga(self.cargar())

        return resultado_etl

    def obt_visitantes(self, dfrm_visitantes):
        return dfrm_visitantes.assign(
            id_cliente=range(1, len(dfrm_visitantes) + 1),  # ID autoincremental
            fuente='Carga_TXT',  # Columna fija
            fecha_proceso=pd.Timestamp.now(),  # Timestamp de ahorita
            email_limpio=dfrm_visitantes['email'].str.strip().str.lower()  # Limpieza básica
        )

    def calculo_visitantes(self, dfrm_visitantes):
        fecha_dt = pd.to_datetime(dfrm_visitantes['Fecha envio'], dayfirst=True)

        return dfrm_visitantes.assign(
            email=dfrm_visitantes['email'].str.strip().str.lower(),
            fechaPrimeraVisita=fecha_dt,
            fechaUltimaVisita=fecha_dt,
            visitasTotales=1,
            visitasAnioActual=1,
            visitasMesActual=1
        )[['email', 'fechaPrimeraVisita', 'fechaUltimaVisita', 'visitasTotales', 'visitasAnioActual',
           'visitasMesActual']]

    def valida_carga(self, salida_del_proceso):
        if salida_del_proceso:
            self.logger.info(f"Archivo procesado con exito {self.archivo_txt}")
            return True
        else:
            self.logger.error(f"La informacion no se pudo cargar a la BD")
            return False
