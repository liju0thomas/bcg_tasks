from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import col, lower, row_number, trim, dense_rank, when, broadcast, lit
import configparser
import constants as k

class Start_spark:
    def __init__(self, concurrent_process):
        """
        Configuring spark
        :param concurrent_process:(Integer) Number of process run in parallel
        """
        self.spark = SparkSession \
                    .builder \
                    .master(f"local[{concurrent_process}]") \
                    .getOrCreate()
    def get_spark(self):
        """
        Configuring spark set up
        :return: spark
        """
        spark = self.spark
        return spark

class Configuration_Setup:
    """
    Configuration set up. This will return path to the input data files and path to target output files
    """
    def __init__(self, path):
        config = configparser.ConfigParser()
        config.read(path)
        self.primary_person = config.get('INPUT_PATH', 'primary_person')
        self.units = config.get('INPUT_PATH', 'units')
        self.damages = config.get('INPUT_PATH', 'damages')
        self.charges = config.get('INPUT_PATH', 'charges')
        self.a1 = config.get('OUTPUT_PATH', 'task1')
        self.a2 = config.get('OUTPUT_PATH', 'task2')
        self.a3 = config.get('OUTPUT_PATH', 'task3')
        self.a4 = config.get('OUTPUT_PATH', 'task4')
        self.a5 = config.get('OUTPUT_PATH', 'task5')
        self.a6 = config.get('OUTPUT_PATH', 'task6')
        self.a7 = config.get('OUTPUT_PATH', 'task7')
        self.a8 = config.get('OUTPUT_PATH', 'task8')
    def return_input_paths(self):
        """
        Get input paths of all datasets
        :return: Strings containing path to input files
        """
        primary_person = self.primary_person
        units = self.units
        damages = self.damages
        charges = self.charges
        return(primary_person,units, damages, charges)

class Input_Output:
    """
    Contains definition of input and output operations
    """
    def get_data(self, path:str, cols:tuple= ('*',), arg_inf_schema:bool= True, arg_hdr:bool=True):
        """
        1.Read csv data as a dataframe
        2. Select only required columns
        3. Return the dataframe
        """
        df = spark.read.csv(path, inferSchema=arg_inf_schema, header=arg_hdr)
        df = df.select(*cols)
        return df

    def write_data(self, path: str, df, mode_to_write:str='overwrite'):
        """
        Writes the dataframe df to a parquet file
        :param path: Path of the parquet file to which output should be written
        :param df: Dataframe
        """
        df.write.parquet(path, mode = mode_to_write)

class PopularItems:
    """
    Contains functions that returns popular things
    eg: top 10 colours, top 10 states etc..
    """
    def popular_10_colors(self, df):
        """
        Returns the top 10 colours of vehicles
        :param df: dataframe
        :return: dataframe
        """
        # Removing these rows as these are not valid colours
        df = df.where(~col('VEH_COLOR_ID').isin('NA', '99', '98'))
        df = df.groupBy('VEH_COLOR_ID').count().orderBy(col('count').desc())
        df = df.select('VEH_COLOR_ID')
        df = df.limit(10)
        return df
    def popular_25_states(self, df):
        """
                Returns the top 25 states
                :param df: dataframe contains data
                :return: dataframe of states
                """
        # Removing these rows as these are not valid states
        df = df.where(~col('DRVR_LIC_STATE_ID').isin('Unknown', 'Other', 'NA'))
        df = df.groupBy('DRVR_LIC_STATE_ID').count().orderBy(col('count').desc())
        df = df.select('DRVR_LIC_STATE_ID')
        df = df.limit(25)
        return df

class Analytic_Tasks:
    """
    Analytics task given for assessment
    """
    def analytics1(self, df):
        """
        Number of crashes in which people killed are male
        :param df: Primary person dataframe
        :return: dataframe with result
        """
        df = df.where(lower(col('PRSN_GNDR_ID'))=='male')
        df = df.where(col('DEATH_CNT')==1)
        df = df.select('CRASH_ID')
        df = df.distinct()
        df = df.groupBy(lit('Total distinct crashes')).count()
        return df

    def analytics3(self, df):
        """
        State with highest number of accidents where females are involved.
        :param df: Primary person dataframe
        :return: dataframe with result
        """
        # Removing rows with DRVR_LIC_STATE_ID as ('na', 'unknown', 'other') as it is not a valid state id
        df = df.where((lower(col('PRSN_GNDR_ID'))== 'female') & (~lower(col('DRVR_LIC_STATE_ID')).isin('na', 'unknown', 'other')))
        df = df.groupBy('DRVR_LIC_STATE_ID').count()
        df = df.orderBy(col('count').desc()).limit(1).select('DRVR_LIC_STATE_ID')
        return df

    def analytics4(self, df):
        """
        Top 5-15 vehicle make ids
        :param df: Units dataframe
        :return: dataframe with result
        """
        df = df.withColumn('tot_casuality_count', col('TOT_INJRY_CNT') + col('DEATH_CNT'))
        df = df.groupBy('VEH_MAKE_ID').sum('tot_casuality_count').withColumnRenamed('sum(tot_casuality_count)',
                                                                                    'sum_casuality_count')
        windowSpec = Window.orderBy(col('sum_casuality_count').desc())
        df = df.withColumn("row_number", dense_rank().over(windowSpec))
        df = df.where((col('row_number') >= 5) & (col('row_number') <= 15))
        df = df.orderBy(col('row_number').asc())
        return df

    def analytics5(self, df, df1):
        """
        Top ethnic user of each unique body style
        :param df: primary person dataframe
        :param df1: units dataframe
        :return: dataframe with results
        """
        df = df.join(df1, on=['CRASH_ID', 'UNIT_NBR'], how='inner')
        df = df.groupBy('VEH_BODY_STYL_ID', 'PRSN_ETHNICITY_ID').count()
        windowSpec = Window.partitionBy('VEH_BODY_STYL_ID').orderBy(col('count').desc())
        df = df.withColumn('rank_body', dense_rank().over(windowSpec))
        df = df.where(col('rank_body') == 1)
        return df

    def analytics6(self, df):
        """
        Top 5 Zip codes with highest crashes due to alcohol
        :param df:Primary person dataframe
        :return: Dataframe with results
        """
        df = df.where(lower(col('PRSN_ALC_RSLT_ID')) == 'positive')
        df = df.where("DRVR_ZIP IS NOT NULL")
        df = df.groupBy('DRVR_ZIP').count()
        df = df.orderBy(col('count').desc()).limit(5)
        return df
    def analytics7(self, df, df1):
        """
        Crash ID where no damage property observed
        :param df: units dataframe
        :param df1: damages dataframe
        :return: dataframe with results
        """
        df = df.withColumn('insurance_claimed', when(lower(col('FIN_RESP_TYPE_ID')).rlike("insurance"), True)
                                                .otherwise(False))
        df = df.where(col('insurance_claimed') == True)
        df1 = df1.where(col('DAMAGED_PROPERTY').isNull())
        df = df.withColumn('severity_of_damage', when(col('VEH_DMAG_SCL_1_ID').rlike("1"), 1)
                           .when(col('VEH_DMAG_SCL_1_ID').rlike("2"), 2)
                           .when(col('VEH_DMAG_SCL_1_ID').rlike("3"), 3)
                           .when(col('VEH_DMAG_SCL_1_ID').rlike("4"), 4)
                           .when(col('VEH_DMAG_SCL_1_ID').rlike("5"), 5)
                           .when(col('VEH_DMAG_SCL_1_ID').rlike("6"), 6)
                           .when(col('VEH_DMAG_SCL_1_ID').rlike("7"), 7)
                           .otherwise(None))
        df = df.where(col('severity_of_damage')>4)
        df = df.join(broadcast(df1), on= 'CRASH_ID', how = 'inner')# broadcasting smaller dataframe for better perfomance
        df = df.groupBy('CRASH_ID').count()
        return df

    def analytics2(self, df):
        """
        Total two wheelers booked for crashes
        :param df: Units dataframe
        :return: Dataframe with results.
        """
        df = df.where((lower(col('VEH_BODY_STYL_ID')) == 'motorcycle') | (lower(col('UNIT_DESC_ID')) == 'pedalcyclist'))
        df = df.groupBy(lit('Two Wheeler count')).count()
        return df

    def analytics8(self, df, df1, df2):
        """
        Top 5 vehicle makers where drivers charged with speeding offenses
        Task 8
        :param df: units dataframe
        :param df1: primary person dataframe
        :param df2: Charges dataframe
        :return: Dataframe with results
        """
        p = PopularItems()
        df_colors = p.popular_10_colors(df)
        df_states = p.popular_25_states(df1)
        df = df.withColumn('is_car', when(lower(col('VEH_BODY_STYL_ID')).rlike("car"), True)
                           .otherwise(False))
        df = df.where(col('is_car') == True)
        df1 = df1.where((lower(col('PRSN_TYPE_ID'))== 'driver') & (lower(col('DRVR_LIC_TYPE_ID')) != 'unlicensed'))
        df2 = df2.withColumn('speed_charges', when(lower(col('CHARGE')).rlike("speed"), True)
                             .otherwise(False))
        df2 = df2.where(col('speed_charges') == True)
        df = df.join(df1, ['CRASH_ID', 'UNIT_NBR'], how='inner')
        df = df.join(df2, ['CRASH_ID', 'UNIT_NBR'], how='inner')
        df = df.join(df_colors, on = 'VEH_COLOR_ID', how = 'inner')
        df = df.join(df_states, on='DRVR_LIC_STATE_ID', how='inner')
        df = df.groupBy('VEH_MAKE_ID').count()
        df = df.limit(5)
        return df

if __name__ == "__main__":
    const = k.Constant_Variables()
    s = Start_spark(const.concurrent_process)
    spark = s.get_spark()
    c = Configuration_Setup(const.config_file_path)
    primary_person, units, damages, charges = c.return_input_paths()
    io = Input_Output()
    df_primary_person = io.get_data(primary_person, const.primary_person_cols)
    df_primary_person.cache()# Caching the df as it is used multiple times
    df_units = io.get_data(units, const.units_cols)
    df_units.cache()# Caching the df as it is used multiple times
    df_damages = io.get_data(damages)
    df_charges = io.get_data(charges, const.charges_cols)
    at = Analytic_Tasks()
    df_an1 = at.analytics1(df_primary_person)
    io.write_data(c.a1, df_an1)
    df_an2 = at.analytics2(df_units)
    io.write_data(c.a2, df_an2)
    df_an3 = at.analytics3(df_primary_person)
    io.write_data(c.a3, df_an3)
    df_an4 = at.analytics4(df_units)
    io.write_data(c.a4, df_an4)
    df_an5 = at.analytics5(df_primary_person, df_units)
    io.write_data(c.a5, df_an5)
    df_an6 = at.analytics6(df_primary_person)
    io.write_data(c.a6, df_an6)
    df_an7 = at.analytics7(df_units, df_damages)
    io.write_data(c.a7, df_an7)
    df_an8 = at.analytics8(df_units, df_primary_person, df_charges)
    io.write_data(c.a8, df_an8)





