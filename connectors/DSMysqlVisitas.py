from sqlalchemy import text


# En tu método cargar() de la clase ETLVisitas:
def cargar(self, lista_ok, lista_errores):
    """
    lista_ok: Lista de dicts con la data limpia.
    lista_errores: Lista de strings con el detalle del error.
    """
    try:
        # Iniciamos transacción con el engine de SQLAlchemy
        with self.db_connector.engine.begin() as conn:

            for fila in lista_ok:
                # --- 1. TABLA: estadistica (INSERT directo) ---
                conn.execute(text("""
                    INSERT INTO estadistica (email, jyv, Badmail, Baja, Fecha_envio, Fecha_open, 
                    Opens, Opens_virales, Fecha_click, Clicks, Clicks_virales, Links, IPs, Navegadores, Plataformas)
                    VALUES (:email, :jk, :Badmail, :Baja, :Fecha_envio, :Fecha_open, 
                    :Opens, :Opens_virales, :Fecha_click, :Clicks, :Clicks_virales, :Links, :IPs, :Navegadores, :Plataformas)
                """), fila)

                # --- 2. TABLA: visitante (UPSERT - La riata del proceso) ---
                # Usamos ON DUPLICATE KEY UPDATE para MariaDB
                conn.execute(text("""
                    INSERT INTO visitante (email, fechaPrimeraVisita, fechaUltimaVisita, visitasTotales, visitasAnioActual, visitasMesActual)
                    VALUES (:email, :Fecha_envio, :Fecha_envio, 1, 1, 1)
                    ON DUPLICATE KEY UPDATE 
                        fechaPrimeraVisita = LEAST(fechaPrimeraVisita, VALUES(fechaPrimeraVisita)),
                        fechaUltimaVisita = GREATEST(fechaUltimaVisita, VALUES(fechaUltimaVisita)),
                        visitasTotales = visitasTotales + 1,
                        visitasAnioActual = visitasAnioActual + 1,
                        visitasMesActual = visitasMesActual + 1
                """), {"email": fila['email'], "Fecha_envio": fila['Fecha_envio']})

            # --- 3. TABLA: errores (Si hubo registros que no pasaron validación) ---
            for error_msg in lista_errores:
                conn.execute(text("""
                    INSERT INTO errores (archivo_origen, detalle, fecha_registro)
                    VALUES (:archivo, :detalle, :fecha)
                """), {
                    "archivo": os.path.basename(self.archivo_txt),
                    "detalle": error_msg,
                    "fecha": datetime.now()
                })

        self.logger.info(f"Carga exitosa en MariaDB para {self.archivo_txt}")
        return True

    except Exception as e:
        self.logger.error(f"Error cargando a MariaDB: {e}")
        return False
