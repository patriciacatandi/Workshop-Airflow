# import libs
import numpy as np
import pandas as pd
import warnings
warnings.filterwarnings('ignore')

def run_preprocessing(
                input_path,
                save_path
                ):

    # Lê os dados
    df = pd.read_csv(input_path, sep=',', encoding='latin-1', nrows=1000)

    # Cria lista com colunas contendo mais de 50% de nans avaliadas no EDA (fixo para não afetar o teste)
    drop_nans = ['albumin_apache', 'bilirubin_apache',
                'fio2_apache', 'paco2_apache',
                'paco2_for_ph_apache', 'pao2_apache',
                'ph_apache', 'urineoutput_apache',
                'd1_diasbp_invasive_max', 'd1_diasbp_invasive_min',
                'd1_mbp_invasive_max', 'd1_mbp_invasive_min',
                'd1_sysbp_invasive_max', 'd1_sysbp_invasive_min',
                'h1_diasbp_invasive_max', 'h1_diasbp_invasive_min',
                'h1_mbp_invasive_max', 'h1_mbp_invasive_min',
                'h1_sysbp_invasive_max', 'h1_sysbp_invasive_min',
                'd1_albumin_max', 'd1_albumin_min',
                'd1_bilirubin_max', 'd1_bilirubin_min',
                'd1_inr_max', 'd1_inr_min',
                'd1_lactate_max', 'd1_lactate_min',
                'h1_albumin_max', 'h1_albumin_min',
                'h1_bilirubin_max', 'h1_bilirubin_min',
                'h1_bun_max', 'h1_bun_min',
                'h1_calcium_max', 'h1_calcium_min',
                'h1_creatinine_max', 'h1_creatinine_min',
                'h1_glucose_max', 'h1_glucose_min',
                'h1_hco3_max', 'h1_hco3_min',
                'h1_hemaglobin_max', 'h1_hemaglobin_min',
                'h1_hematocrit_max', 'h1_hematocrit_min',
                'h1_inr_max', 'h1_inr_min',
                'h1_lactate_max', 'h1_lactate_min',
                'h1_platelets_max', 'h1_platelets_min',
                'h1_potassium_max', 'h1_potassium_min',
                'h1_sodium_max', 'h1_sodium_min',
                'h1_wbc_max', 'h1_wbc_min',
                'd1_arterial_pco2_max', 'd1_arterial_pco2_min',
                'd1_arterial_ph_max', 'd1_arterial_ph_min',
                'd1_arterial_po2_max', 'd1_arterial_po2_min',
                'd1_pao2fio2ratio_max', 'd1_pao2fio2ratio_min',
                'h1_arterial_pco2_max', 'h1_arterial_pco2_min',
                'h1_arterial_ph_max', 'h1_arterial_ph_min',
                'h1_arterial_po2_max', 'h1_arterial_po2_min',
                'h1_pao2fio2ratio_max', 'h1_pao2fio2ratio_min']


    # Cria lista de colunas com pouca variabilidade na informação
    drop_useless = ['icu_stay_type','arf_apache','gcs_unable_apache',
                'aids','cirrhosis','hepatic_failure',
                'immunosuppression','leukemia','lymphoma',
                'solid_tumor_with_metastasis',
               ] 

    # Crialista com colunas categoricas
    categorical_columns = [
                         'apache_2_diagnosis', 'apache_3j_diagnosis',
                         'apache_post_operative',
                         'elective_surgery', 'ethnicity',
                         'gender', 'hospital_admit_source', 'icu_admit_source',
                         'icu_type', 'intubated_apache',  
                         'ventilated_apache'
    ]

    # Colunas que serão dropadas
    drop_cols = [col for col in drop_nans+drop_useless+categorical_columns if col in df.columns]
    df.drop(drop_cols, axis=1, inplace=True)

    # Remove pacientes com menos de 16 anos
    df = df[df['age']>16].reset_index(drop=True)

    # Salva dados processados
    df.to_csv(save_path, index=False, sep=',')