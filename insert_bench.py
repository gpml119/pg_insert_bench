import psycopg as pg
import time
import datetime
import random

with pg.connect(
    host="192.168.3.4", port="15432", user="postgres", password="123",
    dbname='metrics'
) as conn:
    with conn.cursor() as cur:
        sql = """
        insert into weather_metrics (
            time, timezone_shift, city_name, temp_c, feels_like_c,
            temp_min_c, temp_max_c, pressure_hpa, humidity_percent,
            wind_speed_ms, wind_deg, rain_1h_mm, rain_3h_mm,
            snow_1h_mm, snow_3h_mm, clouds_percent, weather_type_id
        )
        values (
            %(time)s, %(timezone_shift)s, %(city_name)s, %(temp_c)s,
            %(feels_like_c)s, %(temp_min_c)s, %(temp_max_c)s, %(pressure_hpa)s,
            %(humidity_percent)s, %(wind_speed_ms)s, %(wind_deg)s,
            %(rain_1h_mm)s, %(rain_3h_mm)s, %(snow_1h_mm)s, %(snow_3h_mm)s,
            %(clouds_percent)s, %(weather_type_id)s
        )
        """
        citys = ['nanjing', 'beijing', 'shanghai', 'hangzhou']

        total_elapse = 0
        date_format_str = '%Y-%m-%d, %H:%M:%S'
        max_loop = 1000
        row_count = 86400

        print('--- insert bench ---\n')
        print('data total rows: {}\n'.format(max_loop * row_count))

        global_begin = time.time()
        global_begin_str = time.strftime(date_format_str,
                                         time.localtime(global_begin))

        for i in range(max_loop):
            print('> {}. insert data({})'.format(i, row_count))
            data = []
            for n in range(row_count):
                delta = datetime.timedelta(seconds=n)
                template = {
                    'time': datetime.datetime(2022, 3, 1, 14, 0, 0) + delta,
                    'timezone_shift': random.randint(1, 100),
                    'city_name': random.choice(citys),
                    'temp_c': random.random() * 100,
                    'feels_like_c': random.random() * 100,
                    'temp_min_c': random.random() * 100,
                    'temp_max_c': random.random() * 100,
                    'pressure_hpa': random.random() * 100,
                    'humidity_percent': random.random() * 100,
                    'wind_speed_ms': random.random() * 100,
                    'wind_deg': random.random() * 100,
                    'rain_1h_mm': random.random() * 100,
                    'rain_3h_mm': random.random() * 100,
                    'snow_1h_mm': random.random() * 100,
                    'snow_3h_mm': random.random() * 100,
                    'clouds_percent': random.randint(0, 100),
                    'weather_type_id': random.randint(0, 100)
                }
                data.append(template)

            begin = time.time()
            begin_str = time.strftime(date_format_str, time.localtime(begin))
            print(' begin: {}'.format(begin_str))

            for n in range(row_count):
                cur.execute(sql, data[n])

            end = time.time()
            end_str = time.strftime(date_format_str, time.localtime(end))
            print('   end: {}'.format(end_str))

            elapse = end - begin
            total_elapse = total_elapse + elapse
            print(' time elapse: {} s'.format(elapse))
            print(' insert speed: %d rows/s\n' % (int(row_count / elapse)))

        global_end = time.time()
        global_end_str = time.strftime(date_format_str,
                                       time.localtime(global_begin))
        print('global begin: {}'.format(global_begin_str))
        print('global end: {}'.format(global_end_str))
        print('total elapsed: {} s'.format(total_elapse))
        avg_speed = int((max_loop * row_count) / total_elapse)
        print('insert speed: %d rows/s' % avg_speed)
