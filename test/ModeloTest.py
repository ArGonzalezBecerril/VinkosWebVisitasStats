import logging
from util.Database import engine
from connectors.Modelo import Base

logger = logging.getLogger("Inicio")
logging.basicConfig(
    level=logging.INFO,
    format='%(levelname)s:%(name)s:%(message)s'
)


logger.info("Creando tablas en MariaDB...")
Base.metadata.create_all(bind=engine)
logging.info("¡Tablas creadas con éxito!")
