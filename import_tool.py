import pandas as pd
from datetime import datetime
from sqlalchemy import create_engine, Integer, String, DateTime
from sqlalchemy.orm import Mapped, mapped_column, sessionmaker, declarative_base
from os import getenv

Base = declarative_base()


class Report(Base):
    __tablename__ = "reports"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.now)
    telegram_user: Mapped[str] = mapped_column(String(100))
    text_conclusions: Mapped[str] = mapped_column(String(1000))
    text_difficult: Mapped[str] = mapped_column(String(1000))

    int_agent_registered: Mapped[int] = mapped_column(Integer, default=0)
    int_all_agent_registered: Mapped[int] = mapped_column(Integer, default=0)
    int_all_model_registered: Mapped[int] = mapped_column(Integer, default=0)
    int_all_model_active: Mapped[int] = mapped_column(Integer, default=0)
    int_lid_accepted: Mapped[int] = mapped_column(Integer, default=0)
    int_lid_active: Mapped[int] = mapped_column(Integer, default=0)
    int_lid_rejected: Mapped[int] = mapped_column(Integer, default=0)
    int_lid_sum: Mapped[int] = mapped_column(Integer, default=0)
    int_model_registered: Mapped[int] = mapped_column(Integer, default=0)


def parse_excel_to_db(excel_file_path: str, db_url: str):
    engine = create_engine(db_url, echo=False)
    Session = sessionmaker(bind=engine)

    try:
        df = pd.read_excel(excel_file_path)
    except Exception as e:
        print(f"Ошибка при чтении Excel файла: {e}")
        return

    df.columns = df.columns.str.strip()

    reports_to_insert = []

    def get_int(row, col_name):
        val = row.get(col_name, 0)
        return int(val) if pd.notna(val) else 0

    def get_str(row, col_name):
        val = row.get(col_name, "")
        return str(val) if pd.notna(val) else ""

    for index, row in df.iterrows():
        dt_val = row.get('Время создания')
        if pd.isna(dt_val):
            dt_val = datetime.now()
        else:
            if isinstance(dt_val, str):
                try:
                    dt_val = pd.to_datetime(dt_val).to_pydatetime()
                except ValueError:
                    dt_val = datetime.now()

        report = Report(
            created_at=dt_val,
            telegram_user=get_str(row, 'телеграм юзернейм'),
            int_lid_sum=get_int(row, 'Вообщем отписанных'),
            int_lid_active=get_int(row, 'Активные диалоги'),
            int_lid_accepted=get_int(row, 'Согласились (Рабоать)'),
            int_lid_rejected=get_int(row, 'Отказы'),
            int_model_registered=get_int(row, 'Моделей записано:'),
            int_agent_registered=get_int(row, 'Агентов записано:'),
            int_all_model_registered=get_int(row, 'Моделей записано:.1'),
            int_all_agent_registered=get_int(row, 'Агентов записано:.1'),
            text_difficult=get_str(row, '❓СЛОЖНОСТИ'),
            text_conclusions=get_str(row, '💭 ВЫВОДЫ:')
        )
        reports_to_insert.append(report)

    # Запись в базу данных
    with Session() as session:
        try:
            session.add_all(reports_to_insert)
            session.commit()
            print(f"Успешно добавлено {len(reports_to_insert)} записей в базу данных!")
        except Exception as e:
            session.rollback()
            print(f"Ошибка при записи в БД: {e}")


# Запуск скрипта
if __name__ == "__main__":
    EXCEL_PATH = "report_data.xlsx"

    DATABASE_URL = getenv("DB_URI")

    parse_excel_to_db(EXCEL_PATH, DATABASE_URL)