# E-mini S&P 500 Futures Tick Data Analysis

Using the series of E-mini S&P 500 futures tick data (available for download [https://azstatpap0001.blob.core.windows.net/hackerrank/QDC/ES.h5?sv=2019-12-12&st=2021-01-21T11%3A09%3A46Z&se=2029-01-22T11%3A09%3A00Z&sr=b&sp=r&sig=%2BjUf7E0wqX%2F0mauFIZRjR25C3ih3umN2ZpC9UlK7a5M%3D]), perform the following transformations and analyses. Provide code for each step.

## Data Preparation

> a. Form a continuous price series by adjusting for rolls.

This is done in the [data_platform.transform.int__es_equity_index_future__continuous](models/equity_index_future/transform/int__es_equity_index_future__continuous.sql) table.
The table can be queried directly in Snowflake, or via the [analyses/1_es_futures_adjusted_prices.py](analyses/1_es_futures_adjusted_prices.py) script.

## Bar Formation

> b. Sample observations by forming:
>
> - Tick bars: Useful for analyzing market activity in terms of trade frequency. Less affected by price changes or trading volume.
> - Volume bars: Analyzing market activity in terms of trading intensity. Can reveal patterns in liquidity.
> - Dollar-traded bars: Useful for analyzing market activity in terms of economic significance.

Sample observations can be found in the following tables:

- [data_platform.transform.int__es_equity_index_future__tick_bars](models/equity_index_future/transform/int__es_equity_index_future__tick_bars.sql)
- [data_platform.transform.int__es_equity_index_future__volume_bars](models/equity_index_future/transform/int__es_equity_index_future__volume_bars.sql)
- [data_platform.transform.int__es_equity_index_future__dollars_traded_bars](models/equity_index_future/transform/int__es_equity_index_future__dollars_traded_bars.sql)

## Analysis Tasks

> c. Bar Count Analysis
>
> - Count the number of bars produced by tick, volume, and dollar bars on a weekly basis.
> - Plot a time series of that bar count.
> - Question: What bar type produces the most stable weekly count? Why?

Weekly bar counts are calculated and aggregated in the [data_platform.fact.timeseries.es_equity_index_future__weekly_bar_counts](models/equity_index_future/fact/timeseries__es_equity_index_future__weekly_bar_counts.sql) table.
The script [analyses/2_weekly_bar_counts.py](analyses/2_weekly_bar_counts.py) can be used to generate the following plot:
![Weekly Bar Counts and Coefficient of Variation by Bar Type](analyses/2_weekly_bar_counts.png)

Volume bars produce the most stable weekly count, likely due to:

- The E-mini S&P 500 is one of the most liquid and heavily traded futures contracts, hence consistent trading volume over time.
- In comparison, tick bars might be more variable because:
- The frequency of trades can fluctuate based on market conditions, for example the graph clearly shows during the 2007-2008 financial crisis, there is a lower weekly bar count for ticks compared to volume or dollars traded bars.
- This suggests that in periods of economic uncertainty, the number of individual trades (ticks) may decrease while the volume and dollar value of trades remain more consistent, possibly due to larger institutional trades or increased volatility.
- Less influenced by price volatility: Unlike dollar bars, volume bars are not directly affected by price changes, which can lead to more stable statistical properties over time.

> d. Serial Correlation Analysis
>
> - Compute the serial correlation of price-returns for the three bar types.
> - Question: What bar method has the lowest serial correlation?

The serial correlation of price returns for tick, volume, and dollar bars is calculated in the [data_platform.fact.timeseries.es_equity_index_future__bar_returns_correlation](models/equity_index_future/fact/timeseries__es_equity_index_future__bar_returns_correlation.sql) table.
The script [analyses/3_bar_returns_correlation.py](analyses/3_bar_returns_correlation.py) can be used to generate the following plot:
![Serial Correlation of Price Returns by Bar Type](analyses/3_bar_returns_correlation.png)

All serial correlation values are very close to zero, which suggests that the returns for all bar types are close to being serially uncorrelated.
The bar method with the lowest serial correlation in absolute terms is tick bars (0.001544), followed by dollar bars (0.013588), and then volume bars (-0.023833).
Tick bars seem to be the most effective at reducing serial correlation, which could make them preferable for certain types of analysis or trading strategies that assume independence between consecutive returns.

> e. Variance Analysis
>
> - Partition the bar series into monthly subsets.
> - Compute the variance of returns for every subset of every bar type.
> - Compute the variance of those variances.
> - Question: What method exhibits the smallest variance of variances?

Monthly bars variances are calculated in the [data_platform.fact.timeseries.es_equity_index_future__monthly_bar_variance](models/equity_index_future/fact/timeseries__es_equity_index_future__monthly_bar_variance.sql) table.
The script [analyses/4_monthly_bar_variance.py](analyses/4_monthly_bar_variance.py) can be used to visualise the monthly bar count variance by bar type as well as the variance of the monthly bar count variances.
![Monthly Bar Variance by Bar Type And Variance of Monthly Bar Count Variances](analyses/4_monthly_bar_variance.png)

Volume bars exhibit the smallest variance of variances, followed by dollars-traded bars, and then tick bars.

> f. Normality Test
>
> - Apply the Jarque-Bera normality test on returns from the three bar types.
> - Question: What method achieves the lowest test statistic?

The Jarque-Bera normality test is applied in the script [analyses/5_jarque_bera_test.py](analyses/5_jarque_bera_test.py).
![Jarque-Bera Test Statistic by Bar Type](analyses/5_jarque_bera_test.png)

Jarque-Bera Test Results:

P-values: < 0.0001 for all bar types, indicating strong evidence against normality.

Test Statistics:

- Tick Bars: 59,435.76
- Volume Bars: 21,175.81
- Dollars Traded Bars: 51,752.98

Interpretation:

- All return distributions significantly deviate from normality.
- Volume bars show the lowest deviation (lowest test statistic).
- Non-normality is common in financial data due to fat tails, skewness, and excess kurtosis.

Conclusion: Bar return distributions are not normal, which is typical for financial return data.

## Summary of Findings

- Bar Count Stability: Volume bars produced the most stable weekly count, likely due to the consistent trading volume of the E-mini S&P 500 futures.
- Serial Correlation: Tick bars showed the lowest serial correlation, making them preferable for analyses assuming independence between consecutive returns.
- Variance Analysis: Volume bars exhibited the smallest variance of variances, indicating more consistent return distributions over time.
- Normality: All bar types showed significant deviation from normality, with volume bars having the lowest Jarque-Bera test statistic.

## Conclusion

This analysis demonstrates that the choice of bar type can significantly impact the statistical properties of financial time series.
Volume bars consistently performed well across multiple metrics, suggesting they may be particularly useful for analyzing the E-mini S&P 500 futures market.
However, each bar type has its strengths, and the optimal choice may depend on the specific application or analysis being performed.
