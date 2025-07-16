import pandas as pd
import psycopg2
import logging
import json
import configparser
from datetime import datetime

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("etl_process.log"),  # Логи будут записываться в файл
        logging.StreamHandler()  # Логи также будут выводиться в консоль
    ]
)


def load_db_config(filename='db_config.ini'):
    """Загрузка конфигурации базы данных из INI-файла."""
    config = configparser.ConfigParser()
    config.read(filename)
    return {
        'source_db': {
            'dbname': config.get('source', 'dbname'),
            'user': config.get('source', 'user'),
            'password': config.get('source', 'password'),
            'host': config.get('source', 'host'),
            'port': config.get('source', 'port')
        },
        'target_db': {
            'dbname': config.get('target', 'dbname'),
            'user': config.get('target', 'user'),
            'password': config.get('target', 'password'),
            'host': config.get('target', 'host'),
            'port': config.get('target', 'port')
        }
    }


def create_target_table(conn):
    """Создание целевой таблицы, если она не существует, и создание индекса для doc_id."""
    logging.info("Создание целевой таблицы, если она не существует")
    create_table_query = """
    CREATE TABLE IF NOT EXISTS target_table (
        doc_id TEXT,
        proc_inst_id_ TEXT,
        contextData TEXT,
        time_ TIMESTAMP
    );
    """
    create_index_query = """
    CREATE INDEX IF NOT EXISTS idx_doc_id ON target_table (doc_id);
    """
    try:
        with conn.cursor() as cursor:
            cursor.execute(create_table_query)
            cursor.execute(create_index_query)
            conn.commit()
            logging.info("Целевая таблица успешно создана или уже существует, индекс для doc_id также создан.")
    except Exception as e:
        logging.error(f"Ошибка при создании целевой таблицы или индекса: {e}")
        raise


def get_max_time(conn):
    """Этап 1: Получение максимального значения time_ из витрины данных."""
    logging.info("Получение максимального значения time_ из витрины данных")
    try:
        with conn.cursor() as cursor:
            cursor.execute("SELECT MAX(time_) FROM public.target_table;")
            max_time = cursor.fetchone()[0]
            if max_time is None:
                logging.info("Целевая таблица пуста, будет извлечено все данные.")
                return '2020-01-01 00:00:00'  # Возвращаем начальное значение для извлечения всех данных
            logging.info(f"Максимальное значение time_: {max_time}")
            return max_time
    except Exception as e:
        logging.error(f"Ошибка при получении максимального значения time_: {e}")
        raise


def extract_data(conn, max_time):
    """Этап 2: Извлечение новых данных из таблицы act_hi_detail."""
    logging.info("Извлечение новых данных из таблицы act_hi_detail")
    # Получение текущей даты
    current_date = datetime.now().date()
    # Формирование имени файла
    output_file = f'output//data_{current_date}.csv'
    try:
        with conn.cursor() as cursor:
            query = f"""
            COPY (
                SELECT proc_inst_id_, text_, name_, time_ 
                FROM act_hi_detail 
                WHERE text_ IS NOT NULL 
                AND name_ IN ('contextData', 'docIds') 
                AND time_ > '{max_time}'
            ) TO STDOUT WITH CSV HEADER DELIMITER ';'
            """
            with open(output_file, 'w') as f:
                cursor.copy_expert(query, f)
            logging.info(f"Данные успешно извлечены в файл: {output_file}")
            return output_file
    except Exception as e:
        logging.error(f"Ошибка при извлечении данных: {e}")
        raise


def transform_data(input_file):
    """Обработка данных с использованием Pandas."""
    logging.info("Начало трансформации данных")
    try:
        df = pd.read_csv(input_file, encoding='windows-1251', delimiter=';')
        results = []

        grouped = df.groupby('proc_inst_id_')
        for proc_inst_id, group in grouped:
            context_data_row = group[group['name_'] == 'contextData']
            context_data = context_data_row['text_'].sample(n=1).values[0] if not context_data_row.empty else None

            doc_ids_row = group[group['name_'] == 'docIds']
            doc_id = None

            if not doc_ids_row.empty:
                doc_ids_text = doc_ids_row['text_'].sample(n=1).values[0]
                try:
                    doc_ids_json = json.loads(doc_ids_text.replace('""', '"'))
                    if isinstance(doc_ids_json, dict) and 'report.AttachedDocuments' in doc_ids_json and 'doc.id' in \
                            doc_ids_json['report.AttachedDocuments'][0]:
                        doc_id = doc_ids_json['report.AttachedDocuments'][0]['doc.id']
                except json.JSONDecodeError:
                    logging.warning(f"Ошибка декодирования JSON для proc_inst_id_: {proc_inst_id}")

            # Добавляем time_ в результаты
            time_ = group['time_'].max() if not group.empty else None

            results.append({
                'doc.id': doc_id,
                'proc_inst_id_': proc_inst_id,
                'contextData': context_data,
                'time_': time_
            })

        result_df = pd.DataFrame(results)
        logging.info(f"Трансформация завершена, получено {len(result_df)} записей")
        return result_df
    except Exception as e:
        logging.error(f"Ошибка при трансформации данных: {e}")
        raise


def load_data(df, conn):
    """Загрузка данных в другую базу данных PostgreSQL."""
    logging.info("Начало загрузки данных в целевую базу данных")
    inserted_count = 0
    try:
        with conn.cursor() as cursor:
            for index, row in df.iterrows():
                # Проверка на наличие None в необходимых полях
                if row['doc.id'] is None or row['proc_inst_id_'] is None or row['contextData'] is None:
                    logging.warning(
                        f"Пропуск записи с индексом {index} {row['proc_inst_id_']} из-за отсутствия необходимых данных.")
                    continue  # Пропустить запись, если одно из полей равно None

                cursor.execute(
                    "INSERT INTO public.target_table (doc_id, proc_inst_id_, contextData, time_) VALUES (%s, %s, %s, %s)",
                    (row['doc.id'], row['proc_inst_id_'], row['contextData'], row['time_'])
                )
                inserted_count += 1
            conn.commit()
            logging.info(f"Данные успешно загружены в целевую базу данных, вставлено записей: {inserted_count}")
    except Exception as e:
        logging.error(f"Ошибка при загрузке данных: {e}")
        raise


def main():
    db_config = load_db_config()
    source_conn = psycopg2.connect(
        dbname=db_config['source_db']['dbname'],
        user=db_config['source_db']['user'],
        password=db_config['source_db']['password'],
        host=db_config['source_db']['host'],
        port=db_config['source_db']['port']
    )
    target_conn = psycopg2.connect(
        dbname=db_config['target_db']['dbname'],
        user=db_config['target_db']['user'],
        password=db_config['target_db']['password'],
        host=db_config['target_db']['host'],
        port=db_config['target_db']['port']
    )

    try:
        # Создание целевой таблицы
        create_target_table(target_conn)

        # ETL процесс
        max_time = get_max_time(target_conn)
        input_file = extract_data(source_conn, max_time)
        transformed_data = transform_data(input_file)
        load_data(transformed_data, target_conn)
    except Exception as e:
        logging.critical(f"Произошла критическая ошибка в ETL процессе: {e}")
    finally:
        source_conn.close()
        target_conn.close()


if __name__ == "__main__":
    main()
