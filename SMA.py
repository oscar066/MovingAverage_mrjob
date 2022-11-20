from mrjob.job import MRJob
from mrjob.step import MRStep
import re


# write a program that calculate a simple moving average of window size 3
# input : [company-symbol][,][date][price]
# output : [company-symbol][,][date][price][,][moving-average]
# SMA = (price1 + price2 + price3) / 3


class SMA(MRJob):
    # defining steps for the job
    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_company,
                reducer=self.reducer_get_company),
            MRStep(reducer=self.reducer_get_sma)
        ]

    # mapper to get company and date,price
    def mapper_get_company(self, _, line):
        # split the line by comma and get company, date, price
        company, date, price = line.split(',')
        # yield company and date,price
        yield company, (date, float(price))

    # reducer to get company and date,price,sma
    def reducer_get_company(self, company, date_price):
        # convert the date_price to list
        date_price = list(date_price)
        # loop through the list
        for i in range(len(date_price) - 2):
            # yield company and date,price,sma
            yield company, (date_price[i][0], date_price[i][1], (date_price[i][1] + date_price[i + 1][1] + date_price[i + 2][1]) / 3)

    # reducer to get company and date,price,sma
    def reducer_get_sma(self, company, date_price_sma):
        # loop through the date_price_sma
        for date, price, sma in date_price_sma:
            # yield company and date ,sma
            yield company, (date, round(sma,2))

if __name__ == '__main__':
    SMA.run()
    