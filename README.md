# ETL-процесс для витрины данных на PostgreSQL
<img width="486" height="486" alt="ETL_camunda_mart" src="https://github.com/user-attachments/assets/1425a9da-24e5-434c-801e-35c8e66dee95" />

## Назначение проекта

Проект направлен на создание витрины данных, чтобы избежать накладных расходов на использование базы данных PostgreSQL, которую использует программное обеспечение Camunda. Основная цель — оптимизировать процесс извлечения данных из исторической таблицы 
act_hi_detail, содержащей более 750 миллионов записей, где данные хранятся в колонке с JSON-структурой.

## Описание таблицы act_hi_detail

Таблица act_hi_detail в Camunda BPM используется для хранения детальной информации о переменных процесса, которые были изменены в процессе выполнения. Она содержит информацию о значениях переменных на разных этапах жизненного цикла процесса, что позволяет анализировать и отлаживать бизнес-процессы, а также отслеживать изменения значений переменных во времени.
Таблица act_hi_detail является важным инструментом для анализа, отладки и аудита бизнес-процессов, предоставляя детальную информацию о изменениях переменных в процессе их выполнения.

## Проблема

Запросы к базе данных, содержащей JSON-данные, требуют значительных временных затрат. Выполнение запроса по одному идентификатору занимает в среднем 20 минут времени, что делает необходимым переход к более эффективным методам обработки данных.
Кроме того, выполнение запросов по нескольким идентификаторам приводит к деградации производительности базы данных. Это может вызвать проблемы с производительностью и доступностью базы данных, так как блокировки могут мешать другим операциям, ожидающим доступа к тем же данным. Важно также сохранить результат выполнения запроса, чтобы избежать повторного выполнения запроса по одному и тому же идентификатору. Это позволит значительно сократить время обработки и снизить нагрузку на базу данных.

## Решение

### Архитектура

- **Извлечение данных**:
  - Использование команды COPY для извлечения данных из PostgreSQL в CSV-файл. Это позволяет извлекать данные более эффективно, чем выполнение отдельных запросов.
- **Инкрементное извлечение данных**:
  - Для оптимизации процесса извлечения данных будет реализовано инкрементное извлечение. Это означает, что данные будут извлекаться на основе максимального значения колонки time_ из предыдущего извлечения.
- **Процесс инкрементного извлечения будет разбит на два этапа**:
  - **Этап 1**: Получение максимального значения time_ из витрины данных:
  ```
  SELECT MAX(time_) FROM your_data_mart;
  ```
  - **Этап 2**: Использование полученного значения для извлечения новых данных из таблицы act_hi_detail:

  ```
  COPY (
      SELECT proc_inst_id_, text_, name_, time_ 
      FROM act_hi_detail 
      WHERE text_ IS NOT NULL 
      AND name_ IN ('contextData', 'docIds') 
      AND time_ > 'значение_из_первого_запроса'
  ) TO '/path/to/your_file.csv' WITH CSV HEADER;
  
  ```
- **Обработка данных**:
- Использование Python и библиотеки Pandas для обработки извлеченных данных. Данные будут загружены из CSV-файла, обработаны и преобразованы в нужный формат.
- **Загрузка данных**:
- После обработки данные будут загружены в другую базу данных PostgreSQL.

### Используемые технологии

- **PostgreSQL**: Основная база данных для хранения исходных данных, используемая программным обеспечением Camunda.
- **Python**: Язык программирования для обработки данных.
- **Pandas**: Библиотека для работы с данными в Python, которая позволяет легко манипулировать и анализировать данные.
- **psycopg2**: Библиотека для взаимодействия с PostgreSQL из Python.

## Шаги по извлечению и обработке данных

### Извлечение данных из PostgreSQL:

- **Этап 1**: Получите максимальное значение time_ из витрины данных:

```
import psycopg2
import logging
import configparser

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

def get_max_time(conn):
    """Этап 1: Получение максимального значения time_ из витрины данных."""
    logging.info("Получение максимального значения time_ из витрины данных")
    try:
        with conn.cursor() as cursor:
            cursor.execute("SELECT MAX(time_) FROM your_data_mart;")
            max_time = cursor.fetchone()[0]
            if max_time is None:
                logging.info("Целевая таблица пуста, будет извлечено все данные.")
                return '2020-01-01 00:00:00'
            logging.info(f"Максимальное значение time_: {max_time}")
            return max_time
    except Exception as e:
        logging.error(f"Ошибка при получении максимального значения time_: {e}")
        raise
```

- **Этап 2**: Используйте полученное значение для извлечения новых данных из таблицы act_hi_detail:

```
COPY (
    SELECT proc_inst_id_, text_, name_, time_ 
    FROM act_hi_detail 
    WHERE text_ IS NOT NULL 
    AND name_ IN ('contextData', 'docIds') 
    AND time_ > 'значение_из_первого_запроса'
) TO '/path/to/your_file.csv' WITH CSV HEADER;
```

### Обработка данных с помощью Pandas:

- Загрузите CSV-файл в DataFrame:

```
import pandas as pd

def transform_data(input_file):
    """Обработка данных с использованием Pandas."""
    logging.info("Начало трансформации данных")
    try:
        df = pd.read_csv(input_file, encoding='utf-8')
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
                    if isinstance(doc_ids_json, dict) and 'report.AttachedDocuments' in doc_ids_json and 'doc.id' in doc_ids_json['report.AttachedDocuments'][0]:
                        doc_id = doc_ids_json['report.AttachedDocuments'][0]['doc.id']
                except json.JSONDecodeError:
                    logging.warning(f"Ошибка декодирования JSON для proc_inst_id_: {proc_inst_id}")
            
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
```

- Выполните необходимые операции по обработке данных, такие как фильтрация, преобразование и агрегация.

### Загрузка данных в другую базу данных PostgreSQL:

- Используйте psycopg2 для подключения к целевой базе данных и загрузки обработанных данных. Пример кода для загрузки данных:

```
import psycopg2

def load_data(df, conn):
    """Загрузка данных в другую базу данных PostgreSQL."""
    logging.info("Начало загрузки данных в целевую базу данных")
    inserted_count = 0
    try:
        with conn.cursor() as cursor:
            for index, row in df.iterrows():
                cursor.execute(
                    "INSERT INTO target_table (doc_id, proc_inst_id_, contextData, time_) VALUES (%s, %s, %s, %s)",
                    (row['doc.id'], row['proc_inst_id_'], row['contextData'], row['time_'])
                )
                inserted_count += 1
            conn.commit()
            logging.info(f"Данные успешно загружены в целевую базу данных, вставлено записей: {inserted_count}")
    except Exception as e:
        logging.error(f"Ошибка при загрузке данных: {e}")
        raise
```

## Заключение

Проект направлен на создание витрины данных для оптимизации извлечения и обработки больших объемов данных из PostgreSQL, используемой программным обеспечением Camunda. Использование подхода с инкрементным извлечением данных, извлечением в CSV и последующей обработкой с помощью Python и Pandas позволяет значительно сократить время выполнения запросов и улучшить производительность системы, а также минимизировать проблемы, связанные с блокировками.
