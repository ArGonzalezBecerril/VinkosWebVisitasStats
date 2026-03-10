from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

# Esto crea el archivo 'visitas.db' en tu carpeta actual
DATABASE_URL = "sqlite:///visitas.db"

engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
