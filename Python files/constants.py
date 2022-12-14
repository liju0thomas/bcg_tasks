class Constant_Variables:
    """
    Used for storing some constant variables
    """
    def __init__(self):
        self.concurrent_process = 3
        self.config_file_path = '..\Config\config.ini'
        self.primary_person_cols = ('CRASH_ID', 'UNIT_NBR', 'PRSN_TYPE_ID', 'PRSN_ETHNICITY_ID',
                                    'PRSN_GNDR_ID', 'DEATH_CNT', 'DRVR_LIC_TYPE_ID','DRVR_LIC_STATE_ID',
                                    'DRVR_ZIP')
        self.charges_cols = ('CRASH_ID', 'UNIT_NBR', 'CHARGE')
        self.units_cols = ('CRASH_ID', 'UNIT_NBR', 'UNIT_DESC_ID', 'VEH_LIC_STATE_ID', 'VEH_COLOR_ID',
                           'VEH_MAKE_ID', 'VEH_BODY_STYL_ID', 'FIN_RESP_TYPE_ID', 'VEH_DMAG_SCL_1_ID',
                           'TOT_INJRY_CNT', 'DEATH_CNT')