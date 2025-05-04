from difflib import get_close_matches
import matplotlib.pyplot as plt
import numpy as np
from builtins import sum as py_sum
from pyspark.sql.functions import (
    col,
    substring,
)

def preprocess_data(data, school_level):
    # drop unnecessary columns
    cols_to_drop = [
        "time_identifier",
        "region_code",
        "new_la_code",
        "old_la_code",
        "year_breakdown",
        "country_code",
        "country_name",
        "all_through",
        "academy_open_date",
        "estab",
        "laestab",
        "urn",
        "geographic_level",
        "sess_authorised_percent_pa_10_exact",
        "sess_overall_pa_10_exact",
        "sess_overall_percent_pa_10_exact",
        "sess_overall_totalreasons",
        "sess_possible",
        "enrolments_pa_10_exact",
        "enrolments_pa_10_exact_percent",
        "sess_authorised_pa_10_exact",
        "sess_possible_pa_10_exact",
        "sess_unauth_holiday",
        "sess_unauth_late",
        "sess_unauth_noyet",
        "sess_unauth_other",
        "sess_unauth_totalreasons",
        "sess_unauthorised_pa_10_exact" "sess_unauthorised_percent_pa_10_exact",
    ]
    clean_data = data.drop(*cols_to_drop)

    # drop rows with local authorities that are no longer valid
    clean_data = clean_data.filter(~(col("la_name").contains("Pre-LGR")))

    # clean extended holiday authorised absences column
    clean_data = clean_data.withColumn(
        "sess_auth_ext_holiday", data.sess_auth_ext_holiday.cast("int")
    )
    clean_data = clean_data.na.fill(value=0, subset=["sess_auth_ext_holiday"])

    # clean time period column
    clean_data = clean_data.withColumn("time_period", substring("time_period", 1, 4))
    clean_data = clean_data.withColumn(
        "time_period", clean_data.time_period.cast("int")
    )

    if school_level == True:
        # only keep rows from the local authority level
        return data.filter(col("geographic_level") == "School")

    # # only keep rows from the local authority level
    # clean_data = clean_data.filter(col("geographic_level") == "Local authority")

    return clean_data.filter(col("geographic_level") == "Local authority")


def get_distinct_strings(df, col_name: str) -> list:
    """
    Get distinct strings in a dataframe column.

    :param df: Spark dataframe.
    :param col_name: Column to search in.
    :returns a list of the distinct strings in the column
    """
    return [row[col_name].upper() for row in df.select(col_name).distinct().collect()]


def check_valid_input(input_str: str, validation_list: list) -> str:
    """
    Checks if a string is in a list.

    :param input_str: String to search for.
    :param validation_list: List to search in.
    :returns input_str: The validated input string.
    """
    input_str = input_str.upper()
    if input_str in validation_list:
        return input_str

    while input_str not in validation_list:
        c = get_close_matches(input_str, validation_list, n=1)
        if input_str == "Q":
            return None
        elif not c:
            input_str = (
                input(
                    f"{input_str} not in list. Please correct your spelling and try again or enter 'Q' to quit: "
                )
                .strip()
                .upper()
            )
        else:
            input_str = (
                input(f"{input_str} not in list. If you meant '{c[0]}', enter 'Y'. ")
                .strip()
                .upper()
            )
            if input_str == "Y":
                input_str = c[0]
    return input_str


def check_valid_year(input_year: int) -> int:
    """
    Checks if a year is included in the data.

    :param input_year: year to check for
    """
    if (input_year >= 2006) and (input_year <= 2018):
        return input_year
    else:
        while not ((input_year >= 2006) and (input_year <= 2018)):
            input_year = int(
                input(
                    f"{input_year} is not a valid year. Please enter a year between 2006 and 2018."
                )
            )
    return input_year


def graph_rate_comparison(df, la1, la2) -> None:
    """
    Graph column values to compare 2 rows in a dataframe.

    :param df: data frame
    :param la1: first row to plot.
    :param la2: second row to plot.
    """
    x = np.arange(len(df.columns))
    df_list = df.collect()

    y1 = df_list[0]
    y2 = df_list[1]
    width = 0.4

    plt.bar(x - 0.2, y1, width)
    plt.bar(x + 0.2, y2, width)
    plt.xticks(x, ["Average Overall", "Average Authorised", "Average Unauthorized"])
    plt.xlabel("Average Absence Rates")
    plt.ylabel("Percent")
    plt.title(f"Average Absence Rates for {la1} and {la2}")
    plt.legend([la1, la2])
    plt.show()


def get_rate_diffs(df, la1, la2) -> None:
    """
    Get the differences in rates between 2 local authorities.

    :param df: data frame
    :param la1: first local authority for comparison
    :param la2: second local authority for comparison
    """
    df_list = df.collect()

    overall_diff = (
        (df_list[0]["avg_absence_rate"] - df_list[1]["avg_absence_rate"])
        / df_list[1]["avg_absence_rate"]
    ) * 100
    auth_diff = (
        (df_list[0]["avg_auth_rate"] - df_list[1]["avg_auth_rate"])
        / df_list[1]["avg_auth_rate"]
    ) * 100
    unauth_diff = (
        (df_list[0]["avg_unauth_rate"] - df_list[1]["avg_unauth_rate"])
        / df_list[1]["avg_unauth_rate"]
    ) * 100

    print(
        f"Overall Absence: {la1} is {overall_diff:.2f}% {'higher' if overall_diff > 0 else 'lower'} than {la2}.\n"
    )
    print(
        f"Authorised Absence: {la1} is {auth_diff:.2f}% {'higher' if auth_diff > 0 else 'lower'} than {la2}.\n"
    )
    print(
        f"Unauthorised Absence: {la1} is {unauth_diff:.2f}% {'higher' if unauth_diff > 0 else 'lower'} than {la2}.\n"
    )


def plot_line_graph(
    df, region_names: list, years: list, x_label: str, y_label: str, title: str
) -> None:
    """
    Plot a line graph.

    :param df: data frame
    :param region_names: list of the names of regions to plot
    :param years: list of years or integers to plot
    :param x_label: string label for the x-axis
    :param y_label: string label for the y-axis
    :param title: string title for the graph
    """
    graph_data = {}

    for row in df:
        if graph_data.get(row["region_name"].upper()) == None:
            graph_data[row["region_name"].upper()] = [row["regional_avg"]]
        else:
            graph_data[row["region_name"].upper()].append(row["regional_avg"])

    plt.figure(figsize=(8, 6))
    for region in region_names:
        plt.plot(years, graph_data.get(region), label=region, marker="o")
        plt.xticks(years)
        plt.xlabel(x_label)
        plt.ylabel(y_label)
        plt.title(title)
        plt.legend()
        plt.grid(True)
    plt.show()


def plot_deviations(df) -> None:
    """
    Plot the deviations between...

    :param df: data frame
    """
    avg_rows = df.select("region_name", "avg_overall_absences").collect()
    avg_rates = [row[1] for row in avg_rows]
    region_names = [row[0] for row in avg_rows]

    national_avg = py_sum(avg_rates) / len(avg_rates)

    deviation = [rate - national_avg for rate in avg_rates]

    # plot bar chart with overall average absence rates
    x = np.arange(len(region_names))
    y = df.select("avg_overall_absences").rdd.flatMap(lambda x: x).collect()
    plt.figure(figsize=(12, 4))
    plt.bar(x, y, 0.6)
    plt.xticks(x + 0.6 / 2, region_names, rotation=45, ha="right")
    plt.axhline(national_avg, color="red", linewidth=0.8)
    # plt.ax
    plt.subplots_adjust(bottom=0.6)
    plt.xlabel("Regions")
    plt.ylabel("Average Absence Rates")
    plt.title("Regional Average Absence Rates (2006-2018)")
    plt.grid(True)
    plt.show()

    plt.figure(figsize=(12, 6))
    plt.tight_layout()
    plt.subplots_adjust(left=0.2)
    plt.barh(
        region_names, deviation, color=["green" if d < 0 else "red" for d in deviation]
    )
    plt.axvline(0, color="black", linewidth=0.8)
    plt.xlabel("Deviation from National Average")
    plt.ylabel("Regions")
    plt.title("Regional Absence Rate Deviations")
    plt.show()


def plot_heatmap(
    df,
    cat1="region_name",
    cat2="school_type",
    num="avg_absence",
    title="Average Absence Rate by Region and School Type",
) -> None:
    """
    Plot heatmap.

    :param df: data frame
    :param cat1: first category/ column of the dataframe to plot values.
    :param cat2: second category/ column of the dataframe to plot values.
    :param num: numeric column with values to use for the map.
    :param title: title for the plot.
    """
    df_list = df.collect()

    regions = sorted(set(row[cat1] for row in df_list))
    st = sorted(set(row[cat2] for row in df_list))

    heatmap_data = np.full((len(regions), len(st)), np.nan)

    region_idx = {region: idx for idx, region in enumerate(regions)}
    st_idx = {st: idx for idx, st in enumerate(st)}

    for row in df_list:
        heatmap_data[region_idx[row[cat1]], st_idx[row[cat2]]] = row[num]

    fig, ax = plt.subplots(figsize=(14, 6))
    cax = ax.imshow(heatmap_data, cmap="coolwarm", interpolation="nearest")

    ax.set_xticks(np.arange(len(st)))
    ax.set_yticks(np.arange(len(regions)))
    ax.set_xticklabels(st, rotation=45, ha="right")
    ax.set_yticklabels(regions)
    plt.subplots_adjust(right=0.9)

    fig.colorbar(cax, ax=ax)

    ax.set_title(title)
    ax.set_xlabel("School Type")
    ax.set_ylabel("Region")
    plt.tight_layout()
    plt.show()
