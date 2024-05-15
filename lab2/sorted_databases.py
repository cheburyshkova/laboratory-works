import pandas as pd

def find_duplicates(data):
    # Сортировка данных по всем полям для выявления возможных дубликатов
    data_sorted = data.sort_values(by=['Date', 'Price', 'Geo_lat', 'Geo_lon', 'Building_type'])

    duplicates = []
    prev_row = None

    for _, row in data_sorted.iterrows():
        if prev_row is None:
            prev_row = row
            continue

        # Проверка на то, насколько текущая строка похожа на предыдущую
        if (row['Date'] == prev_row['Date'] and
            row['Price'] == prev_row['Price'] and
            row['Geo_lat'] == prev_row['Geo_lat'] and
            row['Geo_lon'] == prev_row['Geo_lon'] and
            row['Building_type'] == prev_row['Building_type']):
            duplicates.append((prev_row, row))

        prev_row = row

    return duplicates

def main():
    # Загрузка данных
    merged_data = pd.read_csv('merged_data.csv')

    # Поиск дубликатов
    duplicates = find_duplicates(merged_data)

    # Преобразование результатов в DataFrame
    duplicates_df = pd.DataFrame([{
        'Duplicate1': d[0].to_dict(),
        'Duplicate2': d[1].to_dict()
    } for d in duplicates])

    # Сохранение в CSV файл
    duplicates_df.to_csv('duplicates.csv', index=False)

if __name__ == '__main__':
    main()
