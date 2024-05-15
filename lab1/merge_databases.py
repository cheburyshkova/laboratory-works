import pandas as pd
from mrjob.job import MRJob

class MRBuildingAnalysis(MRJob):

    def mapper(self, _, line):
        parts = line.strip().split(',')
        try:
            date, price, geo_lat, geo_lon, building_type = parts
            
            # Преобразование данных в нужные форматы
            price = int(price)
            geo_lat = float(geo_lat)
            geo_lon = float(geo_lon)
            
            yield building_type, (1, price)
        except ValueError:
            pass  # Игнорируем строки, не подходящие под формат

    def reducer(self, key, values):
        total_count = 0
        total_price = 0

        for count, price in values:
            total_count += count
            total_price += price

        average_price = total_price / total_count if total_count > 0 else 0
        
        yield key, {'average_price': average_price, 'count': total_count}

if __name__ == '__main__':
    # Проверка, запущена ли программа напрямую
    MRBuildingAnalysis.run()

    # Объединяем базы данных в одну
    df1 = pd.read_csv('database1.csv')
    df2 = pd.read_csv('database2.csv')

    # Удаляем лишние столбцы
    columns_to_keep = ['date', 'price', 'geo_lat', 'geo_lon', 'building_type']

    df1 = df1[columns_to_keep]
    df2 = df2[columns_to_keep]

    # Переименовываем столбцы в соответствии с целевой схемой
    df1.columns = ['Date', 'Price', 'Geo_lat', 'Geo_lon', 'Building_type']
    df2.columns = ['Date', 'Price', 'Geo_lat', 'Geo_lon', 'Building_type']

    # Объединяем обе базы данных
    merged_df = pd.concat([df1, df2])

    # Преобразуем столбец 'Date' в формат даты
    merged_df['Date'] = pd.to_datetime(merged_df['Date'], errors='coerce')

    # Сортируем по дате
    merged_df = merged_df.sort_values(by='Date').reset_index(drop=True)

    # Сохраняем результат в CSV файл
    merged_df.to_csv('merged_data.csv', index=False)
