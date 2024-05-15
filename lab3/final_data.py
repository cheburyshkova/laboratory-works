import pandas as pd

def minimum_union(df1, df2):
    # Объединяем данные в один DataFrame
    merged_df = pd.concat([df1, df2])

    # Удаляем дубликаты строк
    merged_df = merged_df.drop_duplicates()

    return merged_df

def main():
    # Загрузка данных
    duplicates_df = pd.read_csv('duplicates.csv')

    # Получаем базы данных, которые были объединены в "duplicates.csv"
    df1 = pd.read_csv('database1.csv')
    df2 = pd.read_csv('database2.csv')

    # Объединяем базы данных
    final_df = minimum_union(df1, df2)

    # Сохранение результата в CSV файл
    final_df.to_csv('final_data.csv', index=False)

if __name__ == '__main__':
    main()
