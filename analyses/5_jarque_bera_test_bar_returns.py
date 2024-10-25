from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
from scipy import stats
from snowflake.snowpark.functions import col

from analyses.snowflake_utils import get_snowflake_connection


def plot_bar_returns_analysis():
    print("Performing Jarque-Bera test on bar returns")

    # Connect to Snowflake
    session = get_snowflake_connection()

    tick_bars = session.table(
        "data_platform.transform.int__es_equity_index_future__tick_bars"
    )
    volume_bars = session.table(
        "data_platform.transform.int__es_equity_index_future__volume_bars"
    )
    dollars_traded_bars = session.table(
        "data_platform.transform.int__es_equity_index_future__dollars_traded_bars"
    )

    # Get data for each bar type
    tick_bars_df = (
        tick_bars.select(col("BAR_START_TIME"), col("CLOSE"))
        .order_by("BAR_START_TIME")
        .to_pandas()
    )

    volume_bars_df = (
        volume_bars.select(col("BAR_START_TIME"), col("CLOSE"))
        .order_by("BAR_START_TIME")
        .to_pandas()
    )

    dollars_traded_bars_df = (
        dollars_traded_bars.select(col("BAR_START_TIME"), col("CLOSE"))
        .order_by("BAR_START_TIME")
        .to_pandas()
    )

    # Calculate returns for each bar type
    def calculate_returns(df):
        df["returns"] = df["CLOSE"].pct_change()
        return df["returns"].dropna()

    tick_returns = calculate_returns(tick_bars_df)
    volume_returns = calculate_returns(volume_bars_df)
    dollars_traded_returns = calculate_returns(dollars_traded_bars_df)

    # Apply Jarque-Bera test
    def jarque_bera_test(returns):
        statistic, p_value = stats.jarque_bera(returns)
        return statistic, p_value

    tick_jb_statistic, tick_jb_p_value = jarque_bera_test(tick_returns)
    volume_jb_statistic, volume_jb_p_value = jarque_bera_test(volume_returns)
    dollars_traded_jb_statistic, dollars_traded_jb_p_value = jarque_bera_test(
        dollars_traded_returns
    )

    # Print results
    print("Jarque-Bera Test Results:")
    print(
        f"Tick Bars: Statistic = {tick_jb_statistic:.4f}, p-value ="
        f" {tick_jb_p_value:.4f}"
    )
    print(
        f"Volume Bars: Statistic = {volume_jb_statistic:.4f}, p-value ="
        f" {volume_jb_p_value:.4f}"
    )
    print(
        f"Dollars Traded Bars: Statistic = {dollars_traded_jb_statistic:.4f}, p-value ="
        f" {dollars_traded_jb_p_value:.4f}"
    )

    # Determine the method with the lowest test statistic
    methods = ["Tick Bars", "Volume Bars", "Dollars Traded Bars"]
    statistics = [tick_jb_statistic, volume_jb_statistic, dollars_traded_jb_statistic]
    lowest_statistic_method = methods[np.argmin(statistics)]

    print(
        "\nThe method with the lowest Jarque-Bera test statistic is:"
        f" {lowest_statistic_method}"
    )

    # Plot histograms and Q-Q plots
    fig, axes = plt.subplots(2, 3, figsize=(15, 10))

    # Histograms
    tick_returns.hist(bins=50, alpha=0.7, ax=axes[0, 0])
    axes[0, 0].set_title("Tick Bars Returns")
    axes[0, 0].set_xlabel("Returns")
    axes[0, 0].set_ylabel("Frequency")

    volume_returns.hist(bins=50, alpha=0.7, ax=axes[0, 1])
    axes[0, 1].set_title("Volume Bars Returns")
    axes[0, 1].set_xlabel("Returns")
    axes[0, 1].set_ylabel("Frequency")

    dollars_traded_returns.hist(bins=50, alpha=0.7, ax=axes[0, 2])
    axes[0, 2].set_title("Dollars Traded Bars Returns")
    axes[0, 2].set_xlabel("Returns")
    axes[0, 2].set_ylabel("Frequency")

    # Q-Q plots
    stats.probplot(tick_returns, dist="norm", plot=axes[1, 0])
    axes[1, 0].set_title("Tick Bars Q-Q Plot")

    stats.probplot(volume_returns, dist="norm", plot=axes[1, 1])
    axes[1, 1].set_title("Volume Bars Q-Q Plot")

    stats.probplot(dollars_traded_returns, dist="norm", plot=axes[1, 2])
    axes[1, 2].set_title("Dollars Traded Bars Q-Q Plot")

    # Save the plot
    script_dir = Path(__file__).parent.resolve()
    plot_name = f"{Path(__file__).stem}.png"
    plot_path = script_dir / plot_name
    plt.savefig(plot_path, bbox_inches="tight")
    print(f"Plot saved as {plot_path}\n\n")
    plt.close()


if __name__ == "__main__":
    plot_bar_returns_analysis()
