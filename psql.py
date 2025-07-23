from sqlalchemy import create_engine, Column, Integer, String, DateTime,Float ,text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from datetime import datetime
import os
from dotenv import load_dotenv
from sqlalchemy.exc import SQLAlchemyError

# Load env vars
load_dotenv()

# --- PostgreSQL Connection Setup ---
DB_URL = f"postgresql://{os.getenv('POSTGRES_USER')}:{os.getenv('POSTGRES_PASSWORD')}@{os.getenv('POSTGRES_HOST')}:{os.getenv('POSTGRES_PORT')}/{os.getenv('POSTGRES_DB')}"
print(f"Connecting to database at {DB_URL}")
engine = create_engine(DB_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

# --- SQLAlchemy Model ---
class StockDetails(Base):
    __tablename__ = 'stock_details'
    id = Column(Integer, primary_key=True, index=True)
    stock_name = Column(String, nullable=False)
    token = Column(String, unique=True, nullable=False)
    ltp = Column(Float, nullable=False)
    last_update = Column(DateTime, nullable=False)

class OhlcData(Base):
    __tablename__ = 'ohlc_data'
    
    id = Column(Integer, primary_key=True, index=True)
    token = Column(String(50), nullable=False)
    start_time = Column(DateTime, nullable=False)
    open = Column(Float, nullable=False)
    high = Column(Float, nullable=False)
    low = Column(Float, nullable=False)
    close = Column(Float, nullable=False)
    interval = Column(String(10), nullable=True)
    created_at = Column(DateTime, server_default=text('CURRENT_TIMESTAMP'), nullable=True)


# --- Create Table if Not Exists ---
# def create_table():
#     Base.metadata.create_all(bind=engine)

# --- Insert or Update Stock Data ---
def insert_data(token, stock_name, ltp):
    session = SessionLocal()
    try:
        stock = session.query(StockDetails).filter_by(token=token).first()
        now = datetime.now()
        if stock:
            stock.ltp = float(ltp)
            stock.stock_name = stock_name
            stock.last_update = now
        else:
            stock = StockDetails(
                token=token,
                stock_name=stock_name,
                ltp=float(ltp),
                last_update=now
            )
            session.add(stock)
        session.commit()
    except Exception as e:
        print(f"DB Error: {e}")
        session.rollback()
    finally:
        session.close()


# ...existing code...

# --- Execute Custom Query ---
def stock_token(params=None):
    """
    Execute a custom SQL SELECT query.

    :param params: Optional dictionary of parameters for the query.
    :return: List of query result rows.
    """
    session = SessionLocal()
    query = '''
    SELECT sd.token, sd.stock_name FROM stock_details sd
    join stocks s on sd.token = s.token
    where s.is_hotlist = true 
    '''
    try:
        result = session.execute(text(query), params or {})
        data={row.token: row.stock_name for row in result.fetchall()}
        # "99926000":"NIFTY50",
        #     "99926009":"BANKNIFTY",
        #     "99926037":"FINNIFTY",
        # data['99926000'] = "NIFTY50"
        # data['99926009'] = "BANKNIFTY"
        # data['99926037'] = "FINNIFTY"

        # print({row.token: row.stock_name for row in result.fetchall()})
        return  data
    except SQLAlchemyError as e:
        print(f"Custom Query Error: {e}")
        return None
    finally:
        session.close()



def insert_ohlc_data(token, start_time, open_, high, low, close, interval=None):
    session = SessionLocal()
    try:
        ohlc_entry = OhlcData(
            token=token,
            start_time=start_time,
            open=float(open_),
            high=float(high),
            low=float(low),
            close=float(close),
            interval=interval
        )
        session.add(ohlc_entry)
        session.commit()
    except SQLAlchemyError as e:
        print(f"Insert OHLC Error: {e}")
        session.rollback()
    finally:
        session.close()


# if __name__ == "__main__":
#     res = execute_custom_query()
#     print(res)
    # for row in res:
    #     print(dict(row._mapping)) 