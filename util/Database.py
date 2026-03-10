from sqlalchemy import create_engine, text
import logging
import pandas as pd

# DATABASE_URL = "mysql://arturo:ntafr@localhost/bd_visitas"
# engine = create_engine(DATABASE_URL)
logging.basicConfig(level=logging.INFO)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(name)s.%(funcName)s | %(message)s')


class WebVinkosDao:
    def __init__(self):
        logging.info("[Ok] Se cargaran a la BD la informacion de Visitas y Estadisticas")

    def obten(self):
        engine = create_engine('mysql+mysqlconnector://usuario:pwd@localhost/bd_visitas')
        return engine

    def inserta_visitas(self, dfrm_web_vinkos):
        logging.info("Iniciando carga de Visitas...")
        try:
            engine = self.obten()
            dfrm_web_vinkos.to_sql('temp_visitante', engine, if_exists='replace', index=False)
            with engine.begin() as conn:
                query = text("""
                                INSERT INTO visitante (email, fechaPrimeraVisita, fechaUltimaVisita, visitasTotales, visitasAnioActual, visitasMesActual)
                                SELECT email, fechaPrimeraVisita, fechaUltimaVisita, visitasTotales, visitasAnioActual, visitasMesActual 
                                FROM temp_visitante
                                ON DUPLICATE KEY UPDATE 
                                    fechaUltimaVisita = VALUES(fechaUltimaVisita),
                                    visitasTotales = visitante.visitasTotales + VALUES(visitasTotales),
                                    visitasAnioActual = visitante.visitasAnioActual + VALUES(visitasAnioActual),
                                    visitasMesActual = visitante.visitasMesActual + VALUES(visitasMesActual);
                            """)
                conn.execute(query)
                conn.execute(text("DROP TABLE IF EXISTS temp_visitante;"))

            logging.info("¡Se cargo la informacion de visitante correctamente")
            return True
        except Exception as e:
            logging.error(f"Error en la carga a PROD: {e}")
            return False

    def inserta_estadistica(self, dfrm_estadisticas):
        logging.info("Iniciando carga de Estadísticas...")
        columnas_map = {
            'email': 'email',
            'jk': 'jv',
            'Badmail': 'Badmail',
            'Baja': 'Baja',
            'Fecha envio': 'Fecha_envio',
            'Fecha open': 'Fecha_open',
            'Opens': 'Opens',
            'Opens virales': 'Opens_virales',
            'Fecha click': 'Fecha_click',
            'Clicks': 'Clicks',
            'Clicks virales': 'Clicks_virales',
            'Links': 'Links',
            'IPs': 'IPs',
            'Navegadores': 'Navegadores',
            'Plataformas': 'Plataformas'
        }

        # Filtramos y renombramos solo lo que nos sirve
        df_estadisticas = dfrm_estadisticas.rename(columns=columnas_map)[columnas_map.values()]

        columnas_fecha = ['Fecha_envio', 'Fecha_open', 'Fecha_click']
        for col in columnas_fecha:
            df_estadisticas[col] = pd.to_datetime(df_estadisticas[col], dayfirst=True, errors='coerce')

        engine = create_engine('mysql+mysqlconnector://usuario:pwd@localhost/bd_visitas')

        try:
            df_estadisticas.to_sql(
                name='estadistica',
                con=engine,
                if_exists='append',
                index=False,
                method='multi'
            )
            logging.info("¡Se cargo la informacion de estadisticas correctamente")
            return True
        except Exception as e:
            logging.error(f"No se cargo la informacion de estadisticas, detalle: {e}")
            return False
