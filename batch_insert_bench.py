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
        values
        """

        citys = ['nanjing', 'beijing', 'shanghai', 'hangzhou']
        total_elapse = 0
        date_format_str = '%Y-%m-%d, %H:%M:%S'
        max_loop = 900
        row_count = 100000
        batch_size = 500

        print('--- batch insert bench ---\n')
        print('data total rows: {}\n'.format(max_loop * row_count))

        global_begin = time.time()
        for loop in range(max_loop):
            print(
                '> {}. row_count: {}, batch_size: {}'.format(
                    loop + 1, row_count, batch_size
                )
            )

            begin = time.time()
            begin_str = time.strftime(date_format_str, time.localtime(begin))
            print(' begin: {}'.format(begin_str))

            for count in range(int(row_count / batch_size)):
                value_str = ''
                for n in range(batch_size):
                    delta = datetime.timedelta(seconds=n)
                    t_str = "('%s',%d,'%s'\
                        ,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%d,%d)" % (
                        str(datetime.datetime(2022, 3, 1, 14, 0, 0) + delta),
                        random.randint(1, 100),
                        random.choice(citys),
                        random.random() * 100,
                        random.random() * 100,
                        random.random() * 100,
                        random.random() * 100,
                        random.random() * 100,
                        random.random() * 100,
                        random.random() * 100,
                        random.random() * 100,
                        random.random() * 100,
                        random.random() * 100,
                        random.random() * 100,
                        random.random() * 100,
                        random.randint(0, 100),
                        random.randint(0, 100)
                        )
                    if n != 0:
                        value_str = value_str + ','
                    value_str += t_str
                full_sql = sql + value_str

                cur.execute(full_sql)

            end = time.time()
            end_str = time.strftime(date_format_str, time.localtime(end))
            print('   end: {}'.format(end_str))

            elapse = end - begin
            total_elapse = total_elapse + elapse
            print(' time elapse: {} s'.format(elapse))
            print(' insert speed: %d rows/s\n' % (int(row_count / elapse)))

        global_end = time.time()
        global_begin_str = time.strftime(date_format_str,
                                         time.localtime(global_begin))
        print('global begin: {}'.format(global_begin_str))

        global_end_str = time.strftime(date_format_str,
                                       time.localtime(global_end))
        print('global   end: {}'.format(global_end_str))
        print('total elapsed: {} s'.format(total_elapse))

        avg_speed = int((max_loop * row_count) / total_elapse)
        print('insert speed: %d rows/s' % avg_speed)
