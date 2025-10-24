# Версия 3.3 - Интегрированный с графиком спредов
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
FORM_COLUMN = 'mrt_archive_total[FormOfSubstance_group]'
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
            print(f"\n⚠ Обнаружено несколько листов в файле:")
            for idx, (sheet_name, sheet_df) in enumerate(excel_data.items(), 1):
                print(f"  {idx}. '{sheet_name}' - {len(sheet_df)} строк, {len(sheet_df.columns)} колонок")
            first_sheet_name = list(excel_data.keys())[0]
            df = excel_data[first_sheet_name]
            print(f"\n→ Используется первый лист: '{first_sheet_name}'")
        else:
            df = excel_data
        print(f"✓ Данные успешно загружены из '{os.path.basename(filename)}'")
        print(f"  ⚠ Пропущены первые 2 строки (заголовки берутся из строки 3)")
        print(f"  Размер датасета: {len(df)} строк, {len(df.columns)} колонок")
        return df
    except Exception as e:
        print(f"✗ Ошибка при загрузке файла: {e}")
        import traceback
        traceback.print_exc()
        return None

# ============= МАТЕМАТИКА ==================

def calculate_standard_correlations(pivot_data):
    location_cols = [col for col in pivot_data.columns if col not in ['Год', 'Неделя', 'time_index']]
    return pivot_data[location_cols].corr()

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
        values=metric,
        index=[year_col, week_col, 'time_index'],
        columns=location_col,
        aggfunc='mean'
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
                base_values = merged['value_base']
                compare_values = merged['value_compare']
                mask = ~(base_values.isna() | compare_values.isna())
                base_clean = base_values[mask]
                compare_clean = compare_values[mask]
                if len(base_clean) >= 2:
                    corr, p_value = pearsonr(base_clean, compare_clean)
                    results[pair_name][f'lag_{lag}'] = {
                        'correlation': corr,
                        'p_value': p_value,
                        'n_points': len(base_clean)
                    }
                else:
                    results[pair_name][f'lag_{lag}'] = {
                        'correlation': np.nan,
                        'p_value': np.nan,
                        'n_points': len(base_clean)
                    }
    return results, pivot

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

# ============= СПРЕДЫ ==================

def calculate_spreads_vs_max(pivot_df):
    """Рассчитывает спреды относительно самой дорогой локации"""
    location_cols = [col for col in pivot_df.columns if col not in ['Год', 'Неделя', 'time_index']]
    if len(location_cols) < 2:
        return None, None
    
    # Находим локацию с максимальной средней ценой
    avg_prices = {loc: pivot_df[loc].mean() for loc in location_cols}
    max_loc = max(avg_prices, key=avg_prices.get)
    
    print(f"  → Базовая локация (самая дорогая): {max_loc} (средняя цена: {avg_prices[max_loc]:.2f})")
    
    # Создаем DataFrame спредов
    spreads_df = pivot_df[['Год', 'Неделя']].copy()
    
    for loc in location_cols:
        if loc != max_loc:
            spread_name = f"{max_loc} - {loc}"
            spreads_df[spread_name] = pivot_df[max_loc] - pivot_df[loc]
    
    print(f"  → Рассчитано {len(location_cols)-1} спредов")
    return spreads_df, max_loc

# =============== ОТЧЕТ ===============

def create_full_excel_report_with_spreads(
    df, output_filename, product_column=PRODUCT_COLUMN, form_column=FORM_COLUMN,
    location_column=LOCATION_COLUMN, year_column=YEAR_COLUMN, week_column=WEEK_COLUMN,
    metrics=METRICS, lags=LAG_RANGE
):
    print("\n" + "=" * 80)
    print("СОЗДАНИЕ ОТЧЕТА С ГРАФИКОМ СПРЕДОВ")
    print("=" * 80)
    
    unique_forms = df[form_column].unique()
    print(f"\nОбнаружены типы продуктов: {', '.join([str(x) for x in unique_forms])}")
    
    # Используем xlsxwriter для графиков
    writer = pd.ExcelWriter(output_filename, engine='xlsxwriter')
    workbook = writer.book
    
    sheet_num = 1
    
    for form_type in sorted(unique_forms):
        df_form = df[df[form_column] == form_type].copy()
        if len(df_form) == 0:
            continue
        
        product_name = df_form[product_column].iloc[0] if product_column in df_form.columns else "Продукт"
        print(f"\n{'='*80}\n{product_name} - {form_type}\n{'='*80}")
        
        safe_form_name = str(form_type).replace(' ', '_').replace('/', '_')[:15]
        safe_product_name = str(product_name).replace(' ', '_').replace('/', '_')[:10]
        
        # Сначала рассчитываем данные для avg
        _, pivot_avg = calculate_lagged_correlations_with_time_index(
            df, form_type=form_type, product_name=product_name, metric='avg',
            lags=lags, year_col=year_column, week_col=week_column,
            location_col=location_column, form_col=form_column
        )
        
        # 1. ЛИСТ SPREADS - САМЫЙ ПЕРВЫЙ!
        if len(pivot_avg) > 0:
            print(f"\n→ Создание листа SPREADS...")
            spreads_df, max_loc = calculate_spreads_vs_max(pivot_avg)
            
            if spreads_df is not None:
                sheet_name = f'{sheet_num}_Spreads_{safe_form_name}'[:31]
                spreads_df.to_excel(writer, sheet_name=sheet_name, index=False, startrow=3)
                worksheet = writer.sheets[sheet_name]
                
                # Заголовок
                header_format = workbook.add_format({
                    'bold': True, 'font_size': 14, 'font_color': 'white',
                    'bg_color': '#4472C4', 'align': 'left', 'valign': 'vcenter'
                })
                title = f"{product_name} {form_type.upper()} - СПРЕДЫ (базовая: {max_loc})"
                worksheet.merge_range('A1:J1', title, header_format)
                
                subtitle_format = workbook.add_format({
                    'italic': True, 'font_size': 11, 'font_color': '#114477', 'align': 'left'
                })
                worksheet.merge_range('A2:J2', f"Расчет по: avg (средняя цена)", subtitle_format)
                
                # График
                chart = workbook.add_chart({'type': 'line'})
                spread_cols = [col for col in spreads_df.columns if col not in ['Год', 'Неделя']]
                
                for col in spread_cols:
                    col_idx = list(spreads_df.columns).index(col)
                    chart.add_series({
                        'name': [sheet_name, 3, col_idx],
                        'categories': [sheet_name, 4, 1, 3 + len(spreads_df), 1],
                        'values': [sheet_name, 4, col_idx, 3 + len(spreads_df), col_idx],
                        'line': {'width': 2},
                    })
                
                chart.set_title({'name': f'{product_name} {form_type}: Спреды относительно {max_loc}'})
                chart.set_x_axis({'name': 'Неделя'})
                chart.set_y_axis({'name': 'Спред (USD/mt)'})
                chart.set_legend({'position': 'bottom'})
                chart.set_size({'width': 1000, 'height': 500})
                
                worksheet.insert_chart('L4', chart)
                worksheet.set_column('A:B', 10)
                worksheet.set_column('C:K', 18)
                
                print(f"✓ Лист {sheet_num}: SPREADS с графиком")
                sheet_num += 1
        
        # 2-N. Остальные листы (как раньше)
        # ... (весь остальной код создания листов)
        
    writer.close()
    print("\n" + "=" * 80)
    print(f"✓ ОТЧЕТ СОЗДАН: '{output_filename}'")
    print("=" * 80)
    return output_filename

def main():
    print("=" * 80)
    print("АНАЛИЗ КОРРЕЛЯЦИЙ + ГРАФИКИ СПРЕДОВ")
    print("Версия 3.3")
    print("=" * 80)
    
    input_file = find_excel_file(INPUT_FOLDER)
    if input_file is None:
        return
    
    df = load_data(input_file, SHEET_NAME)
    if df is None:
        return
    
    required_columns = [PRODUCT_COLUMN, FORM_COLUMN, LOCATION_COLUMN, YEAR_COLUMN, WEEK_COLUMN] + METRICS
    missing_columns = [col for col in required_columns if col not in df.columns]
    if missing_columns:
        print(f"\n✗ Отсутствуют колонки: {missing_columns}")
        return
    
    if not os.path.exists(OUTPUT_FOLDER):
        os.makedirs(OUTPUT_FOLDER)
    
    product_name = df[PRODUCT_COLUMN].iloc[0] if PRODUCT_COLUMN in df.columns else "Product"
    safe_product_name = str(product_name).replace(' ', '_').replace('/', '_')[:20]
    output_filename = f"{safe_product_name}_analysis_with_spreads.xlsx"
    output_path = os.path.join(OUTPUT_FOLDER, output_filename)
    
    create_full_excel_report_with_spreads(
        df=df, output_filename=output_path, product_column=PRODUCT_COLUMN,
        form_column=FORM_COLUMN, location_column=LOCATION_COLUMN,
        year_column=YEAR_COLUMN, week_column=WEEK_COLUMN,
        metrics=METRICS, lags=LAG_RANGE
    )

if __name__ == "__main__":
    main()
