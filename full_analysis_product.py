# Версия 4.1 - ВСЕ ВОЗМОЖНЫЕ КОМБИНАЦИИ СПРЕДОВ
# Correlation + Spreads в ОДНОМ файле

import pandas as pd
import numpy as np
from scipy.stats import pearsonr
import xlsxwriter
import os
import glob

# ============== НАСТРОЙКИ =================

INPUT_FOLDER = './input'
OUTPUT_FOLDER = './output'
SHEET_NAME = None
LAG_RANGE = range(-4, 5)
PRODUCT_COLUMN = 'Группа продукта'
FORM_COLUMN = "'mrt_archive_total'[FormOfSubstance_group]"
LOCATION_COLUMN = 'Локация'
YEAR_COLUMN = 'Год'
WEEK_COLUMN = 'Неделя'
METRICS = ['max', 'avg', 'min']
METRIC_LABELS = {
    'avg': 'avg (средняя цена)',
    'max': 'max (максимальная цена)',
    'min': 'min (минимальная цена)'
}

# ============= ВСПОМОГАТЕЛЬНЫЕ ==================

def find_excel_file(folder_path):
    excel_files = (
        glob.glob(os.path.join(folder_path, '*.xlsx')) +
        glob.glob(os.path.join(folder_path, '*.xls'))
    )
    excel_files = [f for f in excel_files if not os.path.basename(f).startswith('~$')]
    if not excel_files:
        print(f"✗ Не найдено Excel файлов в папке '{folder_path}'")
        return None
    if len(excel_files) > 1:
        print(f"\n⚠ Найдено несколько Excel файлов:")
        for idx, file in enumerate(excel_files, 1):
            print(f"  {idx}. {os.path.basename(file)}")
        print(f"\n→ Используется первый файл: {os.path.basename(excel_files[0])}")
    return excel_files[0]

def load_data(filename, sheet_name=None):
    try:
        excel_data = pd.read_excel(filename, sheet_name=sheet_name, header=2)
        if isinstance(excel_data, dict):
            first_sheet_name = list(excel_data.keys())[0]
            df = excel_data[first_sheet_name]
            print(f"\n→ Используется первый лист: '{first_sheet_name}'")
        else:
            df = excel_data
        print(f"✓ Данные загружены: {len(df)} строк, {len(df.columns)} колонок")
        return df
    except Exception as e:
        print(f"✗ Ошибка: {e}")
        return None

# ============= МАТЕМАТИКА ==================

def calculate_lagged_correlations_with_time_index(df, form_type, product_name, metric='avg',
    lags=range(-4, 5), year_col='Год', week_col='Неделя', location_col='Локация',
    form_col='mrt_archive_total[FormOfSubstance_group]'):
    
    df_filtered = df[df[form_col] == form_type].copy()
    if len(df_filtered) == 0:
        return {}, pd.DataFrame()
    
    df_filtered['time_index'] = (
        df_filtered[year_col].astype(str) + '_W' + df_filtered[week_col].astype(str).str.zfill(2)
    )
    locations = sorted(df_filtered[location_col].unique())
    pivot = df_filtered.pivot_table(
        values=metric, index=[year_col, week_col, 'time_index'],
        columns=location_col, aggfunc='mean'
    ).reset_index()
    pivot = pivot.sort_values([year_col, week_col]).reset_index(drop=True)
    
    results = {}
    for i, base_loc in enumerate(locations):
        for j, compare_loc in enumerate(locations):
            if i >= j:
                continue
            pair_name = f"{base_loc} vs {compare_loc}"
            results[pair_name] = {}
            for lag in lags:
                base_data = pivot[[year_col, week_col, base_loc]].copy()
                base_data.columns = [year_col, week_col, 'value_base']
                compare_data = pivot[[year_col, week_col, compare_loc]].copy()
                compare_data.columns = [year_col, week_col, 'value_compare']
                
                if lag != 0:
                    compare_data['Неделя_shifted'] = compare_data[week_col] + lag
                    compare_data['Год_shifted'] = compare_data[year_col]
                    while (compare_data['Неделя_shifted'] > 52).any():
                        mask = compare_data['Неделя_shifted'] > 52
                        compare_data.loc[mask, 'Год_shifted'] += 1
                        compare_data.loc[mask, 'Неделя_shifted'] -= 52
                    while (compare_data['Неделя_shifted'] < 1).any():
                        mask = compare_data['Неделя_shifted'] < 1
                        compare_data.loc[mask, 'Год_shifted'] -= 1
                        compare_data.loc[mask, 'Неделя_shifted'] += 52
                    merged = base_data.merge(
                        compare_data[['Год_shifted', 'Неделя_shifted', 'value_compare']],
                        left_on=[year_col, week_col],
                        right_on=['Год_shifted', 'Неделя_shifted'],
                        how='inner'
                    )
                else:
                    merged = base_data.merge(compare_data, on=[year_col, week_col], how='inner')
                
                base_values, compare_values = merged['value_base'], merged['value_compare']
                mask = ~(base_values.isna() | compare_values.isna())
                base_clean, compare_clean = base_values[mask], compare_values[mask]
                
                if len(base_clean) >= 2:
                    corr, p_value = pearsonr(base_clean, compare_clean)
                    results[pair_name][f'lag_{lag}'] = {
                        'correlation': corr, 'p_value': p_value, 'n_points': len(base_clean)
                    }
                else:
                    results[pair_name][f'lag_{lag}'] = {
                        'correlation': np.nan, 'p_value': np.nan, 'n_points': len(base_clean)
                    }
    return results, pivot

def calculate_all_spreads(pivot_df):
    """Рассчитывает ВСЕ возможные спреды между локациями (включая обратные)"""
    location_cols = [col for col in pivot_df.columns if col not in ['Год', 'Неделя', 'time_index']]
    
    if len(location_cols) < 2:
        return None
    
    print(f"  → Рассчитываем ВСЕ комбинации спредов для {len(location_cols)} локаций")
    
    spreads_df = pivot_df[['Год', 'Неделя']].copy()
    spread_count = 0
    
    # ВСЕ возможные комбинации (i, j) где i != j
    for i, loc1 in enumerate(location_cols):
        for j, loc2 in enumerate(location_cols):
            if i == j:  # Только избегаем самого себя (A-A)
                continue
            
            # Спред = loc1 - loc2
            spread_name = f"{loc1} - {loc2}"
            spreads_df[spread_name] = pivot_df[loc1] - pivot_df[loc2]
            spread_count += 1
    
    print(f"  → Создано {spread_count} спредов (включая обратные)")
    return spreads_df

def create_correlation_summary_table(corr_results):
    if not corr_results:
        return pd.DataFrame()
    rows = []
    for pair, lags in corr_results.items():
        row = {'Пара локаций': pair}
        for lag_key, values in sorted(lags.items()):
            lag_num = int(lag_key.split('_')[1])
            corr = values['correlation']
            row[f'Lag_{lag_num:+d}'] = round(corr, 3) if not np.isnan(corr) else np.nan
        rows.append(row)
    return pd.DataFrame(rows)

# =============== ОТЧЕТ ===============

def create_full_report(df, output_filename):
    print("\n" + "="*80)
    print("СОЗДАНИЕ ПОЛНОГО ОТЧЕТА (SPREADS ПЕРВЫМИ)")
    print("="*80)
    
    unique_forms = df[FORM_COLUMN].unique()
    writer = pd.ExcelWriter(output_filename, engine='xlsxwriter')
    workbook = writer.book
    
    # Собираем ВСЕ листы в списки
    spreads_sheets = []  # Листы SPREADS
    other_sheets = []    # Все остальные листы
    
    # Рассчитываем все данные
    all_data = {}
    for form_type in sorted(unique_forms):
        df_form = df[df[FORM_COLUMN] == form_type].copy()
        if len(df_form) == 0:
            continue
        
        product_name = df_form[PRODUCT_COLUMN].iloc[0]
        safe_form = str(form_type).replace(' ', '_')[:15]
        safe_product = str(product_name).replace(' ', '_')[:10]
        
        print(f"\n→ Расчет: {product_name} - {form_type}")
        
        form_results, form_pivots = {}, {}
        for metric in METRICS:
            results, pivot = calculate_lagged_correlations_with_time_index(
                df, form_type, product_name, metric, LAG_RANGE,
                YEAR_COLUMN, WEEK_COLUMN, LOCATION_COLUMN, FORM_COLUMN
            )
            form_results[metric] = results
            form_pivots[metric] = pivot
        
        all_data[form_type] = {
            'product_name': product_name,
            'safe_form': safe_form,
            'safe_product': safe_product,
            'form_results': form_results,
            'form_pivots': form_pivots
        }
    
    # Собираем листы SPREADS
    for form_type in sorted(unique_forms):
        if form_type not in all_data:
            continue
        data = all_data[form_type]
        if len(data['form_pivots']['avg']) > 0:
            spreads_df = calculate_all_spreads(data['form_pivots']['avg'])
            if spreads_df is not None:
                spreads_sheets.append({
                    'name': f'SPREADS_{data["safe_form"]}'[:31],
                    'data': spreads_df,
                    'product_name': data['product_name'],
                    'form_type': form_type
                })
    
    # Собираем остальные листы
    for form_type in sorted(unique_forms):
        if form_type not in all_data:
            continue
        data = all_data[form_type]
        
        # AVG, MAX, MIN
        for metric in METRICS:
            if len(data['form_pivots'][metric]) > 0:
                other_sheets.append({
                    'name': f'{data["safe_form"]}_{metric.upper()}'[:31],
                    'data': data['form_pivots'][metric],
                    'type': 'data'
                })
        
        # Корреляции
        if len(data['form_pivots'].get('avg', pd.DataFrame())) > 0:
            # NoLag
            corr_0 = data['form_pivots']['avg'][[col for col in data['form_pivots']['avg'].columns 
                                                 if col not in ['Год', 'Неделя', 'time_index']]].corr()
            other_sheets.append({
                'name': f'{data["safe_form"]}_NoLag'[:31],
                'data': corr_0,
                'type': 'corr'
            })
            
            # Lagged
            lag_table = create_correlation_summary_table(data['form_results']['avg'])
            if len(lag_table) > 0:
                other_sheets.append({
                    'name': f'{data["safe_form"]}_Lagged'[:31],
                    'data': lag_table,
                    'type': 'data'
                })
            
            # Детали по парам
            for pair_name, lags_data in data['form_results']['avg'].items():
                safe_pair = pair_name.replace(' ', '_')[:15]
                detail_data = []
                for lag_key, values in sorted(lags_data.items()):
                    detail_data.append({
                        'Сдвиг': int(lag_key.split('_')[1]),
                        'Корреляция': values['correlation'],
                        'P-value': values['p_value'],
                        'N': values['n_points']
                    })
                other_sheets.append({
                    'name': f'{safe_pair}'[:31],
                    'data': pd.DataFrame(detail_data),
                    'type': 'data'
                })
    
    # ЗАПИСЫВАЕМ ЛИСТЫ В ПРАВИЛЬНОМ ПОРЯДКЕ
    print("\n" + "="*80)
    print("СОЗДАНИЕ ЛИСТОВ")
    print("="*80)
    
    sheet_num = 1
    
    # 1. Сначала ВСЕ SPREADS
    for sheet_info in spreads_sheets:
        sheet_name = f'{sheet_num}_{sheet_info["name"]}'
        sheet_info['data'].to_excel(writer, sheet_name=sheet_name, index=False, startrow=3)
        ws = writer.sheets[sheet_name]
        
        # Заголовок
        hdr_fmt = workbook.add_format({
            'bold': True, 'font_size': 14, 'font_color': 'white',
            'bg_color': '#4472C4', 'align': 'left'
        })
        ws.merge_range('A1:J1', 
                      f"{sheet_info['product_name']} {sheet_info['form_type'].upper()} - ВСЕ СПРЕДЫ", 
                      hdr_fmt)
        sub_fmt = workbook.add_format({'italic': True, 'font_size': 11, 'color': '#114477'})
        ws.merge_range('A2:J2', "Расчет по: avg (средняя цена). Все комбинации локаций", sub_fmt)
        
        # График
        chart = workbook.add_chart({'type': 'line'})
        spread_cols = [col for col in sheet_info['data'].columns if col not in ['Год', 'Неделя']]
        for col in spread_cols:
            col_idx = list(sheet_info['data'].columns).index(col)
            chart.add_series({
                'name': [sheet_name, 3, col_idx],
                'categories': [sheet_name, 4, 1, 3 + len(sheet_info['data']), 1],
                'values': [sheet_name, 4, col_idx, 3 + len(sheet_info['data']), col_idx],
                'line': {'width': 2}
            })
        chart.set_title({'name': f'{sheet_info["product_name"]} {sheet_info["form_type"]}: Все спреды'})
        chart.set_x_axis({'name': 'Неделя'})
        chart.set_y_axis({'name': 'Спред (USD/mt)'})
        chart.set_legend({'position': 'bottom'})
        chart.set_size({'width': 1000, 'height': 500})
        ws.insert_chart('L4', chart)
        ws.set_column('A:B', 10)
        ws.set_column('C:K', 18)
        
        print(f"✓ Лист {sheet_num}: SPREADS (все комбинации)")
        sheet_num += 1
    
    # 2. Затем ВСЕ остальные
    for sheet_info in other_sheets:
        sheet_name = f'{sheet_num}_{sheet_info["name"]}'[:31]
        if sheet_info['type'] == 'corr':
            sheet_info['data'].to_excel(writer, sheet_name=sheet_name)
        else:
            sheet_info['data'].to_excel(writer, sheet_name=sheet_name, index=False)
        print(f"✓ Лист {sheet_num}: {sheet_info['name']}")
        sheet_num += 1
    
    writer.close()
    print("\n" + "="*80)
    print(f"✓ СОЗДАН: '{output_filename}'")
    print("="*80)
    return output_filename

def main():
    print("="*80)
    print("ПОЛНЫЙ АНАЛИЗ: ALL SPREADS + CORRELATIONS")
    print("Версия 4.1")
    print("="*80)
    
    input_file = find_excel_file(INPUT_FOLDER)
    if not input_file:
        return
    
    df = load_data(input_file, SHEET_NAME)
    if df is None:
        return
    
    required = [PRODUCT_COLUMN, FORM_COLUMN, LOCATION_COLUMN, YEAR_COLUMN, WEEK_COLUMN] + METRICS
    missing = [col for col in required if col not in df.columns]
    if missing:
        print(f"✗ Отсутствуют: {missing}")
        return
    
    if not os.path.exists(OUTPUT_FOLDER):
        os.makedirs(OUTPUT_FOLDER)
    
    product_name = df[PRODUCT_COLUMN].iloc[0]
    safe_name = str(product_name).replace(' ', '_')[:20]
    output_path = os.path.join(OUTPUT_FOLDER, f"{safe_name}_FULL_ANALYSIS.xlsx")
    
    create_full_report(df, output_path)
    print(f"\n→ Откройте: {output_path}")

if __name__ == "__main__":
    main()
