# Версия 6.1 - АНАЛИЗ ПО ГРУППАМ ПРОДУКТОВ + СПРЕДЫ ПО ДВ
# Исправлен расчет: делим цену на числовое значение N (не проценты)

import pandas as pd
import numpy as np
from scipy.stats import pearsonr
import xlsxwriter
import os
import glob
import re

# ============== НАСТРОЙКИ =================

INPUT_FOLDER = './input'
OUTPUT_FOLDER = './output'
SHEET_NAME = None
LAG_RANGE = range(-4, 5)
PRODUCT_COLUMN = 'Группа продукта'
FORM_COLUMN_OPTIONS = ["'mrt_archive_total'[FormOfSubstance_group]", "Форма вещества", "FormOfSubstance_group", "Form"]
LOCATION_COLUMN = 'Локация'
YEAR_COLUMN = 'Год'
WEEK_COLUMN = 'Неделя'
METRICS = ['max', 'avg', 'min']
METRIC_LABELS = {
    'avg': 'avg (средняя цена)',
    'max': 'max (максимальная цена)',
    'min': 'min (минимальная цена)'
}

# Словарь сокращений (регистронезависимый)
ABBREVIATIONS = {
    'granular': 'gr',
    'prilled': 'pr',
    'liquid': 'liq',
    'caprolactam': 'capr',
    'standard': 'std',
    'auto-grade': 'au-grd',
    'compacted': 'cmpct'
}

# Словарь действующего вещества (N) для продуктов - ЧИСЛОВЫЕ ЗНАЧЕНИЯ
ACTIVE_SUBSTANCE_N = {
    'AS': 21.0,
    'AN': 34.4,
    'CAN': 20.4,
    'Urea': 46.2,
    'UAN': 30.0,
    'NS': 27.0,
    'ATS': 12.0
}

# ============= ВСПОМОГАТЕЛЬНЫЕ ==================

def get_product_base_name(product_key):
    """Извлекает базовое название продукта (AS, AN, Urea и т.д.)"""
    for product_name in ACTIVE_SUBSTANCE_N.keys():
        if product_key.startswith(product_name):
            return product_name
    return None

def abbreviate_product_name(name):
    """
    Сокращает название продукта согласно словарю сокращений
    Работает универсально с несколькими словами в названии
    """
    if pd.isna(name):
        return name
    
    result = str(name)
    original = result
    
    # Проходим по всем сокращениям и заменяем (case-insensitive)
    for full_word, abbrev in ABBREVIATIONS.items():
        pattern = re.compile(
            r'(?<![a-zA-Z])' + re.escape(full_word) + r'(?![a-zA-Z])', 
            re.IGNORECASE
        )
        result = pattern.sub(abbrev, result)
    
    return result

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

def detect_form_column(df, options):
    """Определяет какая колонка с формой вещества присутствует в данных"""
    for col_name in options:
        if col_name in df.columns:
            print(f"✓ Найдена колонка с формой: '{col_name}'")
            return col_name
    print(f"⚠ Колонка с формой вещества не найдена. Будет использоваться только группа продукта.")
    return None

def create_product_form_key(row, product_col, form_col):
    """Создает уникальный ключ: Продукт или Продукт_Форма"""
    product = str(row[product_col])
    
    if form_col is None or pd.isna(row.get(form_col, np.nan)):
        return product
    
    form = str(row[form_col])
    
    if form == 'nan' or form == '' or form == 'None':
        return product
    else:
        return f"{product}_{form}"

# ============= МАТЕМАТИКА ==================

def calculate_lagged_correlations_by_groups(df, location_filter, metric='avg',
    lags=range(-4, 5), year_col='Год', week_col='Неделя', 
    product_col='Группа продукта', form_col=None, location_col='Локация'):
    """
    Рассчитывает корреляции между различными группами продуктов (с учетом форм)
    для заданной локации
    """
    
    df_filtered = df[df[location_col] == location_filter].copy()
    if len(df_filtered) == 0:
        return {}, pd.DataFrame()
    
    df_filtered['product_form_key'] = df_filtered.apply(
        lambda row: create_product_form_key(row, product_col, form_col), axis=1
    )
    
    df_filtered['time_index'] = (
        df_filtered[year_col].astype(str) + '_W' + df_filtered[week_col].astype(str).str.zfill(2)
    )
    
    groups = sorted(df_filtered['product_form_key'].unique())
    
    print(f"  → Найдено {len(groups)} групп продуктов:")
    for g in groups:
        print(f"     - {g}")
    
    pivot = df_filtered.pivot_table(
        values=metric, 
        index=[year_col, week_col, 'time_index'],
        columns='product_form_key', 
        aggfunc='mean'
    ).reset_index()
    pivot = pivot.sort_values([year_col, week_col]).reset_index(drop=True)
    
    results = {}
    
    for i, base_group in enumerate(groups):
        for j, compare_group in enumerate(groups):
            if i >= j:
                continue
            
            pair_name = f"{base_group} vs {compare_group}"
            results[pair_name] = {}
            
            for lag in lags:
                base_data = pivot[[year_col, week_col, base_group]].copy()
                base_data.columns = [year_col, week_col, 'value_base']
                compare_data = pivot[[year_col, week_col, compare_group]].copy()
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

def calculate_active_substance_prices(pivot_df):
    """
    Рассчитывает цены по действующему веществу (ДВ)
    Делит цену на числовое значение содержания N
    Например: 400 / 34.4 = 11.63 (цена за единицу N)
    """
    group_cols = [col for col in pivot_df.columns if col not in ['Год', 'Неделя', 'time_index']]
    
    print(f"\n  → Рассчитываем цены по ДВ для {len(group_cols)} групп")
    
    dv_pivot = pivot_df[['Год', 'Неделя', 'time_index']].copy()
    converted_count = 0
    skipped_groups = []
    
    for group in group_cols:
        # Определяем базовое название продукта
        base_product = get_product_base_name(group)
        
        if base_product and base_product in ACTIVE_SUBSTANCE_N:
            n_content = ACTIVE_SUBSTANCE_N[base_product]
            # Рассчитываем цену за единицу N: цена / N
            dv_pivot[f"{group}_ДВ"] = pivot_df[group] / n_content
            converted_count += 1
            print(f"    ✓ {group} → {group}_ДВ (N={n_content}, например: 400/{n_content} = {400/n_content:.2f})")
        else:
            skipped_groups.append(group)
    
    print(f"\n  → Конвертировано в ДВ: {converted_count} групп")
    if skipped_groups:
        print(f"  → Пропущено (нет данных о N): {len(skipped_groups)} групп")
        for sg in skipped_groups[:3]:
            print(f"     - {sg}")
    
    return dv_pivot

def calculate_all_spreads(pivot_df, is_dv=False):
    """
    Рассчитывает ВСЕ возможные спреды между группами продуктов (включая обратные)
    С СОКРАЩЕННЫМИ НАЗВАНИЯМИ
    """
    if is_dv:
        group_cols = [col for col in pivot_df.columns if col not in ['Год', 'Неделя', 'time_index'] and col.endswith('_ДВ')]
    else:
        group_cols = [col for col in pivot_df.columns if col not in ['Год', 'Неделя', 'time_index']]
    
    if len(group_cols) < 2:
        return None
    
    print(f"\n  → Рассчитываем ВСЕ комбинации спредов{' по ДВ' if is_dv else ''} для {len(group_cols)} групп")
    
    spreads_df = pivot_df[['Год', 'Неделя']].copy()
    spread_count = 0
    
    # Словарь для отслеживания оригинальных названий
    abbreviation_map = {}
    
    # ВСЕ возможные комбинации (i, j) где i != j - ВКЛЮЧАЯ ОБРАТНЫЕ!
    for i, group1 in enumerate(group_cols):
        for j, group2 in enumerate(group_cols):
            if i == j:  # Только избегаем самого себя (A-A)
                continue
            
            # Сокращаем названия групп
            group1_abbrev = abbreviate_product_name(group1)
            group2_abbrev = abbreviate_product_name(group2)
            
            # Спред = group1 - group2 (с сокращенными названиями)
            spread_name = f"{group1_abbrev} - {group2_abbrev}"
            spreads_df[spread_name] = pivot_df[group1] - pivot_df[group2]
            
            # Сохраняем соответствие для пояснений
            abbreviation_map[spread_name] = (group1, group2)
            
            spread_count += 1
    
    print(f"\n  → Создано {spread_count} спредов (включая обратные комбинации)")
    
    return spreads_df, abbreviation_map

def create_correlation_summary_table(corr_results):
    if not corr_results:
        return pd.DataFrame()
    rows = []
    for pair, lags in corr_results.items():
        row = {'Пара групп': pair}
        for lag_key, values in sorted(lags.items()):
            lag_num = int(lag_key.split('_')[1])
            corr = values['correlation']
            row[f'Lag_{lag_num:+d}'] = round(corr, 3) if not np.isnan(corr) else np.nan
        rows.append(row)
    return pd.DataFrame(rows)

# =============== ОТЧЕТ ===============

def create_full_report(df, output_filename, form_col):
    print("\n" + "="*80)
    print("СОЗДАНИЕ ПОЛНОГО ОТЧЕТА ПО ГРУППАМ ПРОДУКТОВ (SPREADS + ДВ ПЕРВЫМИ)")
    print("="*80)
    
    unique_locations = sorted(df[LOCATION_COLUMN].unique())
    print(f"\n→ Найдено локаций: {len(unique_locations)}")
    print(f"→ Локации: {unique_locations}")
    
    writer = pd.ExcelWriter(output_filename, engine='xlsxwriter')
    workbook = writer.book
    
    # Форматы для пояснений
    header_fmt = workbook.add_format({
        'bold': True, 'font_size': 14, 'font_color': 'white',
        'bg_color': '#4472C4', 'align': 'left', 'valign': 'vcenter'
    })
    header_fmt_dv = workbook.add_format({
        'bold': True, 'font_size': 14, 'font_color': 'white',
        'bg_color': '#70AD47', 'align': 'left', 'valign': 'vcenter'
    })
    subheader_fmt = workbook.add_format({
        'italic': True, 'font_size': 11, 'color': '#114477', 'text_wrap': True
    })
    info_fmt = workbook.add_format({
        'font_size': 10, 'color': '#666666', 'italic': True
    })
    
    spreads_dv_sheets = []  # Листы SPREADS по ДВ
    spreads_sheets = []     # Листы SPREADS обычные
    other_sheets = []       # Все остальные листы
    
    all_data = {}
    
    for location in unique_locations:
        safe_location = str(location).replace(' ', '_').replace('/', '_')[:20]
        
        print(f"\n→ Расчет для локации: {location}")
        
        location_results, location_pivots = {}, {}
        for metric in METRICS:
            results, pivot = calculate_lagged_correlations_by_groups(
                df, location, metric, LAG_RANGE,
                YEAR_COLUMN, WEEK_COLUMN, PRODUCT_COLUMN, form_col, LOCATION_COLUMN
            )
            location_results[metric] = results
            location_pivots[metric] = pivot
        
        all_data[location] = {
            'safe_location': safe_location,
            'location_results': location_results,
            'location_pivots': location_pivots
        }
    
    # Собираем листы SPREADS ПО ДВ (первыми!)
    for location in unique_locations:
        if location not in all_data:
            continue
        data = all_data[location]
        if len(data['location_pivots']['avg']) > 0:
            # Рассчитываем цены по ДВ
            dv_pivot = calculate_active_substance_prices(data['location_pivots']['avg'])
            
            if len([col for col in dv_pivot.columns if col.endswith('_ДВ')]) >= 2:
                spreads_dv_result = calculate_all_spreads(dv_pivot, is_dv=True)
                if spreads_dv_result is not None:
                    spreads_dv_df, abbreviation_map_dv = spreads_dv_result
                    spreads_dv_sheets.append({
                        'name': f'SPREADS_ДВ_{data["safe_location"]}'[:31],
                        'data': spreads_dv_df,
                        'location': location,
                        'abbreviation_map': abbreviation_map_dv,
                        'type': 'spreads_dv'
                    })
    
    # Собираем листы SPREADS обычные
    for location in unique_locations:
        if location not in all_data:
            continue
        data = all_data[location]
        if len(data['location_pivots']['avg']) > 0:
            spreads_result = calculate_all_spreads(data['location_pivots']['avg'])
            if spreads_result is not None:
                spreads_df, abbreviation_map = spreads_result
                group_cols = [col for col in data['location_pivots']['avg'].columns 
                             if col not in ['Год', 'Неделя', 'time_index']]
                spreads_sheets.append({
                    'name': f'SPREADS_{data["safe_location"]}'[:31],
                    'data': spreads_df,
                    'location': location,
                    'groups': group_cols,
                    'abbreviation_map': abbreviation_map,
                    'type': 'spreads'
                })
    
    # Собираем остальные листы
    for location in unique_locations:
        if location not in all_data:
            continue
        data = all_data[location]
        
        # AVG, MAX, MIN
        for metric in METRICS:
            if len(data['location_pivots'][metric]) > 0:
                other_sheets.append({
                    'name': f'{data["safe_location"]}_{metric.upper()}'[:31],
                    'data': data['location_pivots'][metric],
                    'type': 'data',
                    'location': location,
                    'metric': metric
                })
        
        # Корреляции
        if len(data['location_pivots'].get('avg', pd.DataFrame())) > 0:
            # NoLag
            corr_0 = data['location_pivots']['avg'][[col for col in data['location_pivots']['avg'].columns 
                                                 if col not in ['Год', 'Неделя', 'time_index']]].corr()
            other_sheets.append({
                'name': f'{data["safe_location"]}_NoLag'[:31],
                'data': corr_0,
                'type': 'corr',
                'location': location
            })
            
            # Lagged
            lag_table = create_correlation_summary_table(data['location_results']['avg'])
            if len(lag_table) > 0:
                other_sheets.append({
                    'name': f'{data["safe_location"]}_Lagged'[:31],
                    'data': lag_table,
                    'type': 'lagged',
                    'location': location
                })
            
            # Детали по парам
            for pair_name, lags_data in data['location_results']['avg'].items():
                safe_pair = pair_name.replace(' ', '_').replace('/', '_')[:20]
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
                    'type': 'detail',
                    'location': location,
                    'pair_name': pair_name
                })
    
    # ЗАПИСЫВАЕМ ЛИСТЫ В ПРАВИЛЬНОМ ПОРЯДКЕ
    print("\n" + "="*80)
    print("СОЗДАНИЕ ЛИСТОВ С ПОЯСНЕНИЯМИ")
    print("="*80)
    
    sheet_num = 1
    
    # 1. СНАЧАЛА ВСЕ SPREADS ПО ДВ
    for sheet_info in spreads_dv_sheets:
        sheet_name = f'{sheet_num}_{sheet_info["name"]}'
        sheet_info['data'].to_excel(writer, sheet_name=sheet_name, index=False, startrow=5)
        ws = writer.sheets[sheet_name]
        
        # ЗАГОЛОВОК С ПОЯСНЕНИЕМ (зеленый для ДВ)
        ws.merge_range('A1:J1', 
                      f"ЛОКАЦИЯ: {sheet_info['location']} - СПРЕДЫ ПО ДЕЙСТВУЮЩЕМУ ВЕЩЕСТВУ (N)", 
                      header_fmt_dv)
        
        explanation = (f"Цены пересчитаны на действующее вещество (цена / содержание N). "
                      f"Спреды рассчитаны по метрике: {METRIC_LABELS['avg']}. "
                      f"Включены ВСЕ комбинации (A-B и B-A).")
        ws.merge_range('A2:J2', explanation, subheader_fmt)
        
        # Показываем коэффициенты N
        n_info = ", ".join([f"{k}={v}" for k, v in ACTIVE_SUBSTANCE_N.items()])
        ws.merge_range('A3:J3', f"Содержание N (числа): {n_info}", info_fmt)
        
        ws.merge_range('A4:J4', 
                      f"Количество спредов: {len(sheet_info['data'].columns)-2} | "
                      f"Период данных: {sheet_info['data']['Год'].min()}-{sheet_info['data']['Год'].max()}",
                      info_fmt)
        
        # График
        chart = workbook.add_chart({'type': 'line'})
        spread_cols = [col for col in sheet_info['data'].columns if col not in ['Год', 'Неделя']]
        for col in spread_cols[:10]:
            col_idx = list(sheet_info['data'].columns).index(col)
            chart.add_series({
                'name': [sheet_name, 5, col_idx],
                'categories': [sheet_name, 6, 1, 5 + len(sheet_info['data']), 1],
                'values': [sheet_name, 6, col_idx, 5 + len(sheet_info['data']), col_idx],
                'line': {'width': 2}
            })
        chart.set_title({'name': f'{sheet_info["location"]}: Спреды по ДВ'})
        chart.set_x_axis({'name': 'Неделя'})
        chart.set_y_axis({'name': 'Спред ДВ (USD за единицу N)'})
        chart.set_legend({'position': 'bottom'})
        chart.set_size({'width': 1000, 'height': 500})
        ws.insert_chart('L6', chart)
        ws.set_column('A:B', 10)
        ws.set_column('C:K', 24)
        
        print(f"✓ Лист {sheet_num}: SPREADS ПО ДВ для {sheet_info['location']}")
        sheet_num += 1
    
    # 2. ЗАТЕМ ВСЕ SPREADS ОБЫЧНЫЕ
    for sheet_info in spreads_sheets:
        sheet_name = f'{sheet_num}_{sheet_info["name"]}'
        sheet_info['data'].to_excel(writer, sheet_name=sheet_name, index=False, startrow=5)
        ws = writer.sheets[sheet_name]
        
        # ЗАГОЛОВОК С ПОЯСНЕНИЕМ
        ws.merge_range('A1:J1', 
                      f"ЛОКАЦИЯ: {sheet_info['location']} - ВСЕ СПРЕДЫ МЕЖДУ ГРУППАМИ ПРОДУКТОВ", 
                      header_fmt)
        
        abbrev_examples = []
        for abbrev_name, (orig1, orig2) in list(sheet_info['abbreviation_map'].items())[:3]:
            if abbrev_name != f"{orig1} - {orig2}":
                abbrev_examples.append(f"'{abbrev_name}' ← '{orig1}-{orig2}'")
        
        abbrev_text = "; ".join(abbrev_examples) if abbrev_examples else "Нет сокращений в данных"
        
        explanation = (f"Спреды рассчитаны по метрике: {METRIC_LABELS['avg']}. "
                      f"Включены ВСЕ комбинации (A-B и B-A). "
                      f"Применены сокращения: {abbrev_text}")
        ws.merge_range('A2:J2', explanation, subheader_fmt)
        
        ws.merge_range('A3:J3', 
                      f"Группы: {', '.join([abbreviate_product_name(g) for g in sheet_info['groups']])}",
                      info_fmt)
        
        ws.merge_range('A4:J4', 
                      f"Количество спредов: {len(sheet_info['data'].columns)-2} | "
                      f"Период данных: {sheet_info['data']['Год'].min()}-{sheet_info['data']['Год'].max()}",
                      info_fmt)
        
        # График
        chart = workbook.add_chart({'type': 'line'})
        spread_cols = [col for col in sheet_info['data'].columns if col not in ['Год', 'Неделя']]
        for col in spread_cols[:10]:
            col_idx = list(sheet_info['data'].columns).index(col)
            chart.add_series({
                'name': [sheet_name, 5, col_idx],
                'categories': [sheet_name, 6, 1, 5 + len(sheet_info['data']), 1],
                'values': [sheet_name, 6, col_idx, 5 + len(sheet_info['data']), col_idx],
                'line': {'width': 2}
            })
        chart.set_title({'name': f'{sheet_info["location"]}: Спреды между группами'})
        chart.set_x_axis({'name': 'Неделя'})
        chart.set_y_axis({'name': 'Спред (USD/mt)'})
        chart.set_legend({'position': 'bottom'})
        chart.set_size({'width': 1000, 'height': 500})
        ws.insert_chart('L6', chart)
        ws.set_column('A:B', 10)
        ws.set_column('C:K', 22)
        
        print(f"✓ Лист {sheet_num}: SPREADS для {sheet_info['location']} (все комбинации)")
        sheet_num += 1
    
    # 3. Затем ВСЕ остальные
    for sheet_info in other_sheets:
        sheet_name = f'{sheet_num}_{sheet_info["name"]}'[:31]
        
        if sheet_info['type'] == 'data':
            sheet_info['data'].to_excel(writer, sheet_name=sheet_name, index=False, startrow=3)
            ws = writer.sheets[sheet_name]
            ws.merge_range('A1:J1', 
                          f"ЛОКАЦИЯ: {sheet_info['location']} | МЕТРИКА: {METRIC_LABELS[sheet_info['metric']].upper()}",
                          header_fmt)
            ws.merge_range('A2:J2', 
                          f"Данные по всем группам продуктов для локации {sheet_info['location']}",
                          subheader_fmt)
            
        elif sheet_info['type'] == 'corr':
            sheet_info['data'].to_excel(writer, sheet_name=sheet_name, startrow=3)
            ws = writer.sheets[sheet_name]
            ws.merge_range('A1:J1', 
                          f"ЛОКАЦИЯ: {sheet_info['location']} | КОРРЕЛЯЦИЯ БЕЗ СДВИГА (NoLag)",
                          header_fmt)
            ws.merge_range('A2:J2', 
                          "Матрица корреляций Пирсона между группами продуктов (одновременные данные)",
                          subheader_fmt)
            
        elif sheet_info['type'] == 'lagged':
            sheet_info['data'].to_excel(writer, sheet_name=sheet_name, index=False, startrow=3)
            ws = writer.sheets[sheet_name]
            ws.merge_range('A1:J1', 
                          f"ЛОКАЦИЯ: {sheet_info['location']} | КОРРЕЛЯЦИЯ СО СДВИГОМ (Lagged)",
                          header_fmt)
            ws.merge_range('A2:J2', 
                          "Корреляции с временными сдвигами от -4 до +4 недель",
                          subheader_fmt)
            
        elif sheet_info['type'] == 'detail':
            sheet_info['data'].to_excel(writer, sheet_name=sheet_name, index=False, startrow=3)
            ws = writer.sheets[sheet_name]
            ws.merge_range('A1:J1', 
                          f"ДЕТАЛИ: {sheet_info['pair_name']}",
                          header_fmt)
            ws.merge_range('A2:J2', 
                          f"Детальная корреляция для локации {sheet_info['location']} с временными сдвигами",
                          subheader_fmt)
        
        print(f"✓ Лист {sheet_num}: {sheet_info['name']}")
        sheet_num += 1
    
    writer.close()
    print("\n" + "="*80)
    print(f"✓ СОЗДАН: '{output_filename}'")
    print("="*80)
    return output_filename

def main():
    print("="*80)
    print("ПОЛНЫЙ АНАЛИЗ: SPREADS + SPREADS ПО ДВ + CORRELATIONS")
    print("Версия 6.1 - Расчет по действующему веществу (делим на число)")
    print("="*80)
    print(f"\nСловарь сокращений:")
    for full, short in ABBREVIATIONS.items():
        print(f"  {full} → {short}")
    print(f"\nСодержание действующего вещества N (числа, не проценты):")
    for product, n_value in ACTIVE_SUBSTANCE_N.items():
        print(f"  {product} = {n_value} (пример: 400/{n_value} = {400/n_value:.2f})")
    
    input_file = find_excel_file(INPUT_FOLDER)
    if not input_file:
        return
    
    df = load_data(input_file, SHEET_NAME)
    if df is None:
        return
    
    print(f"\n→ Доступные колонки: {list(df.columns)}")
    
    form_col = detect_form_column(df, FORM_COLUMN_OPTIONS)
    
    required = [PRODUCT_COLUMN, LOCATION_COLUMN, YEAR_COLUMN, WEEK_COLUMN] + METRICS
    missing = [col for col in required if col not in df.columns]
    if missing:
        print(f"✗ Отсутствуют обязательные колонки: {missing}")
        return
    
    if not os.path.exists(OUTPUT_FOLDER):
        os.makedirs(OUTPUT_FOLDER)
    
    # УНИВЕРСАЛЬНОЕ ИМЯ ФАЙЛА БЕЗ TIMESTAMP
    unique_locations = sorted(df[LOCATION_COLUMN].unique())
    all_locations_str = "_".join([str(loc).replace(' ', '').replace('/', '')[:10] for loc in unique_locations])
    
    output_path = os.path.join(OUTPUT_FOLDER, f"{all_locations_str}_PRODUCT_GROUPS_ANALYSIS.xlsx")
    
    create_full_report(df, output_path, form_col)
    print(f"\n→ Откройте: {output_path}")

if __name__ == "__main__":
    main()
