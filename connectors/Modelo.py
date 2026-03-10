from sqlalchemy import Column, String, Integer, DateTime, Text, BigInteger
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

class Visitante(Base):
    __tablename__ = "visitante"
    email = Column(String(150), primary_key=True)
    fechaPrimeraVisita = Column(DateTime)
    fechaUltimaVisita = Column(DateTime)
    visitasTotales = Column(Integer, default=0)
    visitasAnioActual = Column(Integer, default=0)
    visitasMesActual = Column(Integer, default=0)

class Estadistica(Base):
    __tablename__ = "estadistica"
    id = Column(BigInteger, primary_key=True, autoincrement=True)
    email = Column(String(150))
    jyv = Column(String(50))
    Badmail = Column(String(10))
    Baja = Column(String(10))
    Fecha_envio = Column(DateTime)
    Fecha_open = Column(String(50))
    Opens = Column(Integer)
    Opens_virales = Column(Integer)
    Fecha_click = Column(String(50))
    Clicks = Column(Integer)
    Clicks_virales = Column(Integer)
    Links = Column(Text)
    IPs = Column(Text)
    Navegadores = Column(Text)
    Plataformas = Column(Text)

class RegistroError(Base):
    __tablename__ = "errores"
    id = Column(Integer, primary_key=True, autoincrement=True)
    archivo_origen = Column(String(200))
    detalle = Column(Text)
    fecha_registro = Column(DateTime)
