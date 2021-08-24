from portfolio_analyzer import PortfolioAnalyzer
from pandas import ExcelWriter

if __name__ == '__main__':
    analyzer = PortfolioAnalyzer('data/transactions.csv')
    df_yearly = analyzer.execute('year')
    df_monthly = analyzer.execute('month')
    df_quarterly = analyzer.execute('quarter')
    writer = ExcelWriter('output/portfolio_analysis.xlsx')
    df_yearly.to_excel(writer, 'yearly_analysis')
    df_monthly.to_excel(writer, 'monthly_analysis')
    df_quarterly.to_excel(writer, 'quarterly_analysis')
    writer.save()
