from pyspark.sql.functions import (
    col,
    upper,
    avg,
    sum,
    round,
)

import util


def search_enrolments(
    df,
    loc_col="la_name",
    year_col="time_period",
    num_col="enrolments",
    st_col="school_type",
) -> None:
    """
    Search dataset for number of pupil enrolments by local authority and year.

    :param df: data frame with pupil enrolment data by geographical region, type of school
    :param loc_col: Location column in data frame.
    :param year_col: Year or time period column in data frame.
    :param num_col: Numerical column in data frame (default is enrolments).
    :param st_col: School type column in data frame.
    """
    temp = df.select(col(loc_col), col(year_col), col(num_col)).filter(
        col(st_col) == "Total"
    )
    la_names = util.get_distinct_strings(temp, loc_col)

    authorities = input(
        "Enter Local Authority name(s) to search for or 'ALL'. If providing a list, please separate names with commas. "
    ).strip()

    # filter for user requested authorities and show
    if authorities.upper() == "ALL":
        # show all authorities
        temp.groupby(*[loc_col, year_col]).sum(num_col).sort(
            col(loc_col), col(year_col)
        ).show(n=temp.count(), truncate=False)
    else:
        authorities_list = [a.strip().upper() for a in authorities.split(",")]
        for a in authorities_list:
            input_a = util.check_valid_input(a, la_names)
            if input_a != a:
                authorities_list.remove(a)
                authorities_list.append(input_a)
        # show filtered authorities
        temp.filter(upper(col(loc_col)).isin(authorities_list)).sort(
            col(loc_col), col(year_col)
        ).show(n=temp.count(), truncate=False)


def search_authorized_absences(
    df,
    st="school_type",
    year_col="time_period",
    abs_count="sess_authorised",
    total_reasons="sess_auth_totalreasons",
    reasons_cols=[
        "sess_auth_appointments",
        "sess_auth_excluded",
        "sess_auth_ext_holiday",
        "sess_auth_holiday",
        "sess_auth_traveller",
        "sess_auth_illness",
        "sess_auth_religious",
        "sess_auth_other",
        "sess_auth_study",
    ],
) -> None:
    """
    Search dataset for authorised absences by school type and year. Option to view further breakdown of
    specific types of authorised absences.

    :param df: data frame
    :param st: column name with school types
    :param t: column name with time period/ years
    :param abs_count: column name with absence counts
    :param total_reasons: column name with total reasons
    :param reasons_cols: list of column names that are authorised absence reasons
    """
    temp = df.select(
        col(st), col(year_col), col(abs_count), col(total_reasons), *reasons_cols
    )

    school_types = util.get_distinct_strings(temp, st)

    school_type = input("Enter school type to search for: ").strip().upper()

    st_valid = util.check_valid_input(school_type, school_types)

    year = int(input("Enter the school year to search for: ").strip())
    valid_year = util.check_valid_year(year)

    filtered = temp.filter(upper(col(st)) == st_valid).filter(
        col(year_col) == valid_year
    )

    filtered.select(col(st), col(year_col), col(abs_count)).groupby(*[st, st]).agg(
        sum("sess_authorised").alias("total_authorised_absences")
    ).show(truncate=False)

    extended_option = input(
        "Would you like to see the breakdown of types of authorized absenses? \n"
        "Enter 'Y' for yes or press 'Enter' for no: "
    )

    # break down authorised absences by reason
    if extended_option == "Y" or extended_option == "y":
        filtered = filtered.withColumn(
            "unknown_reasons", col(abs_count) - col(total_reasons)
        )
        filtered.groupby(*[st, year_col]).agg(
            sum("sess_auth_appointments").alias("medical_reasons"),
            sum("sess_auth_excluded").alias("excluded_absences"),
            sum("sess_auth_ext_holiday").alias("extended_holiday"),
            sum("sess_auth_holiday").alias("holiday"),
            sum("sess_auth_traveller").alias("traveller_absences"),
            sum("sess_auth_illness").alias("illness"),
            sum("sess_auth_religious").alias("religious_reasons"),
            sum("sess_auth_study").alias("study_leave"),
            sum("sess_auth_other").alias("other"),
            sum("unknown_reasons").alias("unknown"),
        ).show(truncate=False, vertical=True)


def search_unauthorized_absences(
    df,
    r="region_name",
    la="la_name",
    year_col="time_period",
    unauth_col="sess_unauthorised",
) -> None:
    """
    Search dataset for total unauthorised absences by year and either region or local authority.

    :param df: data frame
    """
    temp = df.select(col(la), col(r), col(year_col), col(unauth_col)).filter(
        col("school_type") == "Total"
    )
    choice = input(
        "Enter 'R' to break down by region or 'LA' to break down by local authority: "
    )
    year = int(input("Enter school year to search for: "))
    valid_year = util.check_valid_year(year)

    if choice.upper() == "R":
        temp.select(col(r), col(year_col), col(unauth_col)).filter(
            col(year_col).isin(valid_year)
        ).groupby(r).sum(unauth_col).show(n=temp.count(), truncate=False)
    elif choice.upper() == "LA":
        temp.select(col(la), col(unauth_col)).filter(
            col(year_col).isin(valid_year)
        ).show(n=temp.count(), truncate=False)


def compare_two_authorities(df) -> None:
    """
    Compare 2 LAs in a given year.

    :param df: data frame
    """
    temp = df.select(
        col("la_name"),
        col("time_period"),
        col("num_schools"),
        col("enrolments"),
        col("sess_overall"),
        col("sess_overall_percent"),
        col("sess_authorised_percent"),
        col("sess_unauthorised_percent"),
    ).filter(col("school_type") == "Total")
    la_names = util.get_distinct_strings(df, "la_name")
    la1 = input("Enter the first local authority for your comparison: ").strip().upper()
    la1_valid = util.check_valid_input(la1, la_names)

    la2 = (
        input("Enter the second local authority for your comparison: ").strip().upper()
    )
    la2_valid = util.check_valid_input(la2, la_names)

    school_year = input("Enter the school year to search for: ")

    comp = (
        temp.filter(
            (
                (upper(col("la_name")) == la2_valid)
                | (upper(col("la_name")) == la1_valid)
            )
            & (col("time_period") == school_year)
        )
        .drop("time_period")
        .groupby("la_name")
        .agg(
            sum("num_schools").alias("total_schools"),
            sum("enrolments").alias("total_enrolments"),
            sum("sess_overall").alias("total_absences"),
            avg("sess_overall_percent").alias("avg_absence_rate"),
            avg("sess_authorised_percent").alias("avg_auth_rate"),
            avg("sess_unauthorised_percent").alias("avg_unauth_rate"),
        )
    )
    comp.show(truncate=False)

    util.get_rate_diffs(
        comp.select(
            col("avg_absence_rate"), col("avg_auth_rate"), col("avg_unauth_rate")
        ),
        la1_valid,
        la2_valid,
    )

    util.graph_rate_comparison(
        comp.select(
            col("avg_absence_rate"), col("avg_auth_rate"), col("avg_unauth_rate")
        ),
        la1_valid,
        la2_valid,
    )


def performance_exploration(df) -> None:
    """
    Explore the changes in average attendance rates by region between 2006 and 2018.

    :param df: dataframe
    """
    filtered = (
        df.select(col("region_name"), col("time_period"), col("sess_overall_percent"))
        .filter(col("school_type") == "Total")
        .groupby("region_name", "time_period")
        .agg(avg("sess_overall_percent").alias("regional_avg"))
        .sort("region_name", "time_period")
    )

    region_names = util.get_distinct_strings(filtered, "region_name")
    years = filtered.select(col("time_period")).distinct().sort("time_period").collect()
    years = [row["time_period"] for row in years]

    # plot line graph for avg. absence rates over time
    util.plot_line_graph(
        filtered.collect(),
        region_names,
        years,
        "Year",
        "Average Percent Absences",
        "Regional Average Absence Rates between 2006 and 2018",
    )

    slope_df = filtered.filter(
        (col("time_period") == int(years[0])) | (col("time_period") == int(years[-1]))
    ).collect()

    # plot slope graph for overall change
    util.plot_line_graph(
        slope_df,
        region_names,
        # np.arange(2),
        [years[0], years[-1]],
        "Year",
        "Average Percent Absences",
        "Regional Average Absence Rate Change from 2006 to 2018",
    )

    # get overall average absence rates
    overall_avgs = (
        filtered.select(col("region_name"), col("regional_avg"))
        .groupby("region_name")
        .agg(avg("regional_avg").alias("avg_overall_absences"))
        .sort("avg_overall_absences")
    )
    overall_avgs = overall_avgs.withColumn(
        "avg_overall_absences", round(overall_avgs["avg_overall_absences"], 4)
    )
    overall_avgs.show(truncate=False)

    util.plot_deviations(overall_avgs)


def explore_links(df, df_school) -> None:
    """
    Explore if there is a link between school type, location, and absences.

    :param df: data frame
    """
    temp = (
        df.select(col("region_name"), col("school_type"), col("sess_overall_percent"))
        .filter(col("school_type") != "Total")
        .groupby("region_name", "school_type")
        .agg(avg("sess_overall_percent").alias("avg_absence"))
        .sort("region_name")
    )
    temp.show(n=temp.count(), truncate=False)

    util.plot_heatmap(temp)

    temp_school = (
        df_school.select(
            col("region_name"),
            col("school_type"),
            col("academy_type"),
            col("sess_overall_percent"),
        )
        .filter(col("school_type") == "Special")
        .drop(col("school_type"))
        .groupby("region_name", "academy_type")
        .agg(avg("sess_overall_percent").alias("avg_absence"))
        .dropna()
        .sort("region_name")
    )
    util.plot_heatmap(
        temp_school,
        cat2="academy_type",
        title="Average Absence Rate by Region and Academy Type (Special Schools)",
    )
