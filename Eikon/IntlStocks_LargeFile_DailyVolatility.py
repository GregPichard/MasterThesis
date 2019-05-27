import os
import datetime
def FilterLines(in_filename, out_filename, key):
    with open(in_filename) as in_f, open(out_filename, 'w') as out_f:
        #N_rows = sum(1 for line in in_f)
        #print("Number of rows in", in_filename, ": ", N_rows)
        for line in in_f:
            if line.find(key) > -1:
                out_f.write(line)

def main():
    start_time = datetime.datetime.now()
    in_filename = "./Daily/IntlStockPrice-volume_Series_daily_db.csv"
    #out_filename = "./Daily/IntlStocks_Close_Daily_db.csv"
    out_filename = "./Daily/IntlStocks_Volume_Daily_db.csv"
    #key = "CLOSE" # For Close price
    key = "VOLUME" # Another need : extract daily volume before monthly aggregation
    FilterLines(in_filename, out_filename, key)
    open(in_filename).close()
    open(out_filename).close()
    elapsed = datetime.datetime.now() - start_time
    print("Time elapsed: ", elapsed)

if __name__ == "__main__":
    main()
    
