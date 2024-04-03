import json
import sys
from mrjob.job import MRJob

class MRBookData1Transform(MRJob):
    string_cache = ''

    def mapper(self, _, line):
        line = line.strip()
        if line == '{':
            MRBookData1Transform.string_cache = line
        elif line == '}' or line == '},':
            MRBookData1Transform.string_cache += '}'
            data = json.loads(MRBookData1Transform.string_cache)
            yield data['BookTitle'], json.dumps(data)
        elif line != '[' and line != ']':
            MRBookData1Transform.string_cache += line

    def reducer(self, key, values):
        for value in values:
            old_data = json.loads(value)
            new_data = MRBookData1Transform.transform(old_data)
            yield key, new_data

    @staticmethod
    def transform(old_data):
        new_data = {
            'Название': old_data['BookTitle'],
            'Жанр': old_data['Genre'],
            'Автор': old_data['Author'],
            'Год': int(old_data['Year']) if old_data['Year'] and str(old_data['Year']).isdigit() else 0
        }
        return new_data

class MRBookData2Transform(MRJob):
    string_cache = ''

    def mapper(self, _, line):
        line = line.strip()
        if line == '{':
            MRBookData2Transform.string_cache = line
        elif line == '}' or line == '},':
            MRBookData2Transform.string_cache += '}'
            data = json.loads(MRBookData2Transform.string_cache)
            yield data['title'], json.dumps(data)
        elif line != '[' and line != ']':
            MRBookData2Transform.string_cache += line

    def reducer(self, key, values):
        for value in values:
            old_data = json.loads(value)
            new_data = MRBookData2Transform.transform(old_data)
            yield key, new_data

    @staticmethod
    def transform(old_data):
        genres = old_data['genres'].split(',')
        genre = genres[0] if genres else ''
        new_data = {
            'Название': old_data['title'],
            'Жанр': genre,
            'Автор': old_data['authors'],
            'Год': int(old_data['year']) if old_data['year'].isdigit() else 0
        }
        return new_data

if __name__ == '__main__':
    args = sys.argv[1:]
    new_values = []

    for arg in args:
        if arg == 'data1':
            job = MRBookData1Transform(args=[f'{arg}.json'])
        else:
            job = MRBookData2Transform(args=[f'{arg}.json'])
        with job.make_runner() as runner:
            runner.run()
            for key, value in job.parse_output(runner.cat_output()):
                new_values.append(value)
    
    with open('merged_books.json', 'w') as file:
        print(json.dumps(new_values, indent=4, ensure_ascii=False), file=file)