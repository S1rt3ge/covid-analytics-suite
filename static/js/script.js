let isLoading = false;

// Определяем группы стран
const countryGroups = {
    europe: ['Germany', 'France', 'Italy', 'Spain', 'United Kingdom'],
    northamerica: ['United States', 'Canada'],
    asia: ['Japan', 'China', 'India'],
    all: ['Germany', 'France', 'Italy', 'Spain', 'United Kingdom', 'United States', 'Canada', 'Japan', 'China', 'India', 'Brazil', 'Australia']
};

const colors = ['#6366f1', '#8b5cf6', '#06b6d4', '#10b981', '#f59e0b', '#ef4444', '#8b5a2b', '#6b7280', '#ec4899', '#84cc16', '#f97316', '#0ea5e9'];

// Функция для выбора группы стран
function selectCountryGroup(groupName) {
    const select = document.getElementById('countries');
    const buttons = document.querySelectorAll('.country-btn');
    // Сначала очищаем все выборы
    for (let option of select.options) {
        option.selected = false;
    }
    // Убираем активный класс со всех кнопок
    buttons.forEach(btn => btn.classList.remove('active'));
    if (groupName === 'clear') {
        updateSelectVisual();
        return;
    }
    // Выбираем страны группы
    if (countryGroups[groupName]) {
        for (let option of select.options) {
            if (countryGroups[groupName].includes(option.value)) {
                option.selected = true;
            }
        }
    }
    // Добавляем активный класс к соответствующей кнопке
    const activeButton = document.querySelector(`[data-group="${groupName}"]`);
    if (activeButton) {
        activeButton.classList.add('active');
    }
    updateSelectVisual();
}

// Функция для обновления визуального отображения селекта
function updateSelectVisual() {
    const select = document.getElementById('countries');
    const selectedValues = Array.from(select.selectedOptions).map(o => o.value);
    // Обновляем стили опций
    for (let option of select.options) {
        if (option.selected) {
            option.style.backgroundColor = '#6366f1';
            option.style.color = 'white';
        } else {
            option.style.backgroundColor = '';
            option.style.color = '';
        }
    }
    // Обновляем стили кнопок на основе выбранных стран
    const buttons = document.querySelectorAll('.country-btn');
    buttons.forEach(btn => btn.classList.remove('active'));
    // Проверяем какие группы полностью выбраны
    Object.keys(countryGroups).forEach(groupName => {
        const groupCountries = countryGroups[groupName];
        const isGroupFullySelected = groupCountries.every(country => selectedValues.includes(country));
        if (isGroupFullySelected) {
            const button = document.querySelector(`[data-group="${groupName}"]`);
            if (button) {
                button.classList.add('active');
            }
        }
    });
}

// Update year display
document.getElementById('year').addEventListener('input', function() {
    const yearDisplay = document.getElementById('year-value');
    const y = this.value;
    document.getElementById('start-date').value = `${y}-01-01`;
    document.getElementById('end-date').value   = `${y}-12-31`;
    yearDisplay.style.transform = 'scale(1.2)';
    yearDisplay.textContent = this.value;
    setTimeout(() => {
        yearDisplay.style.transform = 'scale(1)';
    }, 200);
});

// Обновляем визуал при изменении селекта
document.getElementById('countries').addEventListener('change', function() {
    updateSelectVisual();
});

async function checkHealth() {
    const statusElement = document.getElementById('health-status');
    console.log("🔍 checkHealth called");
    try {
        const response = await fetch('/health?verbose=0', { cache: 'no-cache' });
        console.log("📡 Health response status:", response.status);
        if (!response.ok) throw new Error('Network response was not ok');
        const data = await response.json();
        console.log("✅ Health ", data);
        const isOnline = data.snowflake && data.mongodb;
        const sfStatus = data.snowflake ? '✓' : '✗';
        const mongoStatus = data.mongodb ? '✓' : '✗';
        if (isOnline) {
            statusElement.className = 'status-indicator status-online';
            statusElement.innerHTML = `<i class="fas fa-check-circle"></i> System Online - SF: ${sfStatus} | Mongo: ${mongoStatus}`;
        } else {
            statusElement.className = 'status-indicator status-warning';
            statusElement.innerHTML = `<i class="fas fa-exclamation-triangle"></i> Partial - SF: ${sfStatus} | Mongo: ${mongoStatus}`;
        }
    } catch (error) {
        console.error("💥 checkHealth error:", error);
        statusElement.className = 'status-indicator status-offline';
        statusElement.innerHTML = '<i class="fas fa-times-circle"></i> System Offline';
    }
}

async function fetchWithErrorHandling(url, options = {}) {
    try {
        const response = await fetch(url, options);
        if (!response.ok) {
            const errorData = await response.json().catch(() => ({ detail: 'Unknown error' }));
            throw new Error(`${errorData.detail || 'Request failed'} (Status: ${response.status})`);
        }
        return await response.json();
    } catch (error) {
         console.error(`Fetch error for ${url}:`, error);
         throw error;
    }
}

async function updateCharts() {
    if (isLoading) return;
    isLoading = true;
    const button = document.getElementById('update-btn');
    const originalText = button.innerHTML;
    button.innerHTML = '<div class="loading"></div> Updating...';
    button.disabled = true;
    try {
        const selectedOptions = document.getElementById('countries').selectedOptions;
        let countries = Array.from(selectedOptions).map(o => o.value);
        if (countries.length === 0) {
            selectCountryGroup('europe');
            countries = ['Germany', 'France'];
        }
        const year = document.getElementById('year').value;
        const startDate = document.getElementById('start-date').value;
        const endDate = document.getElementById('end-date').value;
        const caseType = document.getElementById('case-type').value;
        const chartIds = ['daily-deaths', 'analytics', 'infection-rate', 'demography'];
        chartIds.forEach(id => {
             document.getElementById(id).innerHTML = '<div style="display: flex; align-items: center; justify-content: center; height: 400px;"><div class="loading"></div></div>';
        });
        // --- Загружаем данные по дневным смертям ---
        const dailyData = [];
        const dailyPromises = countries.map((country, i) => 
            fetchWithErrorHandling(`/covid/daily_deaths?country=${encodeURIComponent(country)}&year=${year}`)
                .then(data => {
                    if (data.series && data.series.length > 0) {
                        const trace = {
                            x: data.series.map(item => item.date),
                            y: data.series.map(item => item.deaths),
                            name: country,
                            type: 'scatter',
                            mode: 'lines+markers',
                            line: {
                                color: colors[i % colors.length],
                                width: 3,
                                shape: 'spline'
                            },
                            marker: {
                                size: 4,
                                color: colors[i % colors.length]
                            },
                            hovertemplate: '<b>%{fullData.name}</b><br>Date: %{x}<br>Deaths: %{y}<extra></extra>'
                        };
                        dailyData.push(trace);
                    }
                })
                .catch(e => console.error(`Error for ${country} daily deaths:`, e))
        );
        await Promise.allSettled(dailyPromises);
        const dailyLayout = {
            title: {
                text: `Daily COVID Deaths - ${year}`,
                font: { size: 18, color: '#f8fafc', family: 'Inter' },
                x: 0.5
            },
            xaxis: {
                title: 'Date',
                gridcolor: 'rgba(255,255,255,0.1)',
                color: '#cbd5e1'
            },
            yaxis: {
                title: 'Daily Deaths',
                gridcolor: 'rgba(255,255,255,0.1)',
                color: '#cbd5e1'
            },
            plot_bgcolor: 'transparent',
            paper_bgcolor: 'transparent',
            font: { family: 'Inter', color: '#cbd5e1' },
            legend: {
                orientation: 'h',
                y: -0.2,
                x: 0.5,
                xanchor: 'center'
            },
            hovermode: 'x unified',
            margin: { t: 60, l: 60, r: 20, b: 80 }
        };
        const config = {
            responsive: true,
            displayModeBar: false,
            scrollZoom: false
        };
        if (dailyData.length > 0) {
            Plotly.newPlot('daily-deaths', dailyData, dailyLayout, config);
        } else {
            document.getElementById('daily-deaths').innerHTML = `
                <div style="display: flex; align-items: center; justify-content: center; height: 400px; color: var(--warning);">
                    <i class="fas fa-exclamation-triangle" style="font-size: 3rem; margin-right: 1rem;"></i>
                    <div>
                        <h4>No data available</h4>
                        <p>No daily death data found for selected countries and year.</p>
                    </div>
                </div>
            `;
        }
        // --- Загружаем Analytics данные ---
        try {
            const analyticsUrl = `/analytics/mortality-vs-gdp?year=${year}&countries=${encodeURIComponent(countries.join(','))}`;
            const analyticsData = await fetchWithErrorHandling(analyticsUrl);
            if (analyticsData.sample && analyticsData.sample.length > 0) {
                const sample = analyticsData.sample;
                const hasValidData = sample.some(s => s.deaths_per_100k != null && !isNaN(s.deaths_per_100k));
                if (!hasValidData) throw new Error("All mortality values are invalid.");
                const analyticsTrace = {
                    x: sample.map(item => item.country),
                    y: sample.map(item => item.deaths_per_100k),
                    type: 'bar',
                    name: 'Deaths per 100k',
                    marker: {
                        color: sample.map((_, i) => colors[i % colors.length]),
                        opacity: 0.8,
                        line: { color: 'rgba(255,255,255,0.2)', width: 1 }
                    },
                    text: sample.map(it =>
                        (it.deaths_per_100k == null || isNaN(it.deaths_per_100k) || it.deaths_per_100k < 0.05) ? '≈0' : Number(it.deaths_per_100k).toFixed(2)
                    ),
                    textposition: 'auto',
                    hovertemplate:
                        '<b>%{x}</b><br>Deaths per 100k: %{y:.2f}<br>GDP per capita: %{customdata:$,.0f}<extra></extra>',
                    customdata: sample.map(it => it.gdp_per_capita)
                };
                let analyticsTitle;
                if (countries.length > 0 && countries.length <= 12) {
                    analyticsTitle = `Mortality Rate — ${year} (Selected Countries)`;
                } else {
                    analyticsTitle = `Top 10 Countries by Mortality Rate — ${year}`;
                }
                const analyticsLayout = {
                    title: {
                        text: analyticsTitle,
                        font: { size: 18, color: '#f8fafc', family: 'Inter' },
                        x: 0.5
                    },
                    xaxis: {
                        title: 'Country',
                        gridcolor: 'rgba(255,255,255,0.1)',
                        color: '#cbd5e1',
                        tickangle: -45
                    },
                    yaxis: {
                        title: 'Deaths per 100k Population',
                        gridcolor: 'rgba(255,255,255,0.1)',
                        color: '#cbd5e1'
                    },
                    plot_bgcolor: 'transparent',
                    paper_bgcolor: 'transparent',
                    font: { family: 'Inter', color: '#cbd5e1' },
                    margin: { t: 60, l: 60, r: 20, b: 120 }
                };
                document.getElementById('analytics').innerHTML = '';
                await new Promise(resolve => setTimeout(resolve, 100));
                Plotly.newPlot('analytics', [analyticsTrace], analyticsLayout, config);
                // ---------- Infection Rate (per 100k) ----------
                try {
                    const popByCountry = {};
                    sample.forEach(r => {
                        if (r.country && r.population) popByCountry[r.country] = r.population;
                    });
                    if (Object.keys(popByCountry).length === 0) {
                        throw new Error("No population metadata available.");
                    }
                    const infPromises = countries.map(c => {
                        const pop = popByCountry[c];
                        if (!pop) return Promise.resolve(null);
                        const url = `/covid/summary?country=${encodeURIComponent(c)}&date_from=${startDate}&date_to=${endDate}&case_type=confirmed`;
                        return fetchWithErrorHandling(url)
                            .then(js => ({ country: c, value: js.value, population: pop }))
                            .catch(() => null);
                    });
                    const infResults = await Promise.allSettled(infPromises);
                    const infSeries = infResults
                        .filter(result => result.status === 'fulfilled' && result.value)
                        .map(result => result.value)
                        .map(data => ({
                            country: data.country,
                            per100k: data.population > 0 ? (data.value / data.population) * 100000 : 0
                        }))
                        .filter(item => isFinite(item.per100k));
                    if (infSeries.length) {
                        const infTrace = {
                            x: infSeries.map(x => x.country),
                            y: infSeries.map(x => x.per100k),
                            type: 'bar',
                            name: 'Infections per 100k',
                            marker: {
                                color: infSeries.map((_, i) => colors[i % colors.length]),
                                opacity: 0.85
                            },
                            hovertemplate: '<b>%{x}</b><br>Infections per 100k: %{y:.2f}<extra></extra>',
                            text: infSeries.map(x => (x.per100k < 0.05 ? '≈0' : x.per100k.toFixed(2))),
                            textposition: 'auto'
                        };
                        const infLayout = {
                            title: {
                                text: `Infection Rate — ${startDate} → ${endDate} (Selected Countries)`,
                                font: { size: 18, color: '#f8fafc', family: 'Inter' },
                                x: 0.5
                            },
                            xaxis: { title: 'Country', gridcolor: 'rgba(255,255,255,0.1)', color: '#cbd5e1', tickangle: -45 },
                            yaxis: { title: 'Infections per 100k', gridcolor: 'rgba(255,255,255,0.1)', color: '#cbd5e1' },
                            plot_bgcolor: 'transparent',
                            paper_bgcolor: 'transparent',
                            font: { family: 'Inter', color: '#cbd5e1' },
                            margin: { t: 60, l: 60, r: 20, b: 120 }
                        };
                        document.getElementById('infection-rate').innerHTML = '';
                        Plotly.newPlot('infection-rate', [infTrace], infLayout, config);
                    } else {
                        throw new Error("No valid infection data found for the selected range.");
                    }
                } catch (e) {
                    console.error('Infection rate error:', e);
                    document.getElementById('infection-rate').innerHTML = `
                        <div style="display:flex;align-items:center;justify-content:center;height:400px;color:var(--warning);">
                            <i class="fas fa-exclamation-triangle" style="font-size:3rem;margin-right:1rem;"></i>
                            <div>
                                <h4>Infection Rate Unavailable</h4>
                                <p>${e.message}</p>
                            </div>
                        </div>`;
                }
                // ---------- Demographic Breakdown (by GDP buckets) ----------
                try {
                    const rows = sample.filter(r => r.deaths_per_100k != null && !isNaN(r.deaths_per_100k) && r.gdp_per_capita != null && !isNaN(r.gdp_per_capita));
                    if (rows.length === 0) throw new Error("No valid data for GDP breakdown.");
                    const buckets = [
                        { name: 'GDP < $10k',  test: v => v < 10000 },
                        { name: '$10k–$30k',   test: v => v >= 10000 && v < 30000 },
                        { name: '>$30k',       test: v => v >= 30000 }
                    ];
                    const labels = [];
                    const values = [];
                    const texts  = [];
                    buckets.forEach(b => {
                        const grp = rows.filter(r => b.test(Number(r.gdp_per_capita)));
                        const n = grp.length;
                        if (n > 0) {
                            const avg = grp.reduce((s, r) => s + Number(r.deaths_per_100k), 0) / n;
                            texts.push(`${b.name}<br>Avg deaths per 100k: ${avg.toFixed(2)}<br>Countries: ${n}`);
                            values.push(avg);
                        } else {
                            texts.push(`${b.name}<br>No countries in this bucket`);
                            values.push(null);
                        }
                        labels.push(b.name);
                    });
                    const demoTrace = {
                        x: labels,
                        y: values,
                        type: 'bar',
                        marker: { color: colors.slice(0, labels.length), opacity: 0.9 },
                        text: values.map(v => (v == null || isNaN(v) ? 'no data' : (v < 0.05 ? '≈0' : v.toFixed(2)))),
                        textposition: 'auto',
                        hoverinfo: 'text',
                        hovertext: texts
                    };
                    const demoLayout = {
                        title: { text: `Mortality by GDP Group — ${year}`, font: { size: 18, color: '#f8fafc', family: 'Inter' }, x: 0.5 },
                        xaxis: { title: 'GDP per capita group', gridcolor: 'rgba(255,255,255,0.1)', color: '#cbd5e1' },
                        yaxis: { title: 'Avg deaths per 100k', gridcolor: 'rgba(255,255,255,0.1)', color: '#cbd5e1' },
                        plot_bgcolor: 'transparent',
                        paper_bgcolor: 'transparent',
                        font: { family: 'Inter', color: '#cbd5e1' },
                        margin: { t: 60, l: 60, r: 20, b: 80 }
                    };
                    document.getElementById('demography').innerHTML = '';
                    Plotly.newPlot('demography', [demoTrace], demoLayout, config);
                } catch (e) {
                    console.error('Demography error:', e);
                    document.getElementById('demography').innerHTML = `
                        <div style="display:flex;align-items:center;justify-content:center;height:400px;color:var(--warning);">
                            <i class="fas fa-exclamation-triangle" style="font-size:3rem;margin-right:1rem;"></i>
                            <div>
                                <h4>Demographic Breakdown Unavailable</h4>
                                <p>${e.message}</p>
                            </div>
                        </div>`;
                }
            } else {
                throw new Error("Analytics data is empty.");
            }
        } catch (e) {
            console.error('Analytics block error:', e);
            document.getElementById('analytics').innerHTML = `
                <div style="display: flex; align-items: center; justify-content: center; height: 400px; color: var(--error);">
                    <i class="fas fa-exclamation-circle" style="font-size: 3rem; margin-right: 1rem;"></i>
                    <div>
                        <h4>Analytics Error</h4>
                        <p>${e.message}</p>
                    </div>
                </div>
            `;
            document.getElementById('infection-rate').innerHTML = `
                <div style="display:flex;align-items:center;justify-content:center;height:400px;color:var(--text-muted);">
                    <i class="fas fa-virus" style="font-size:3rem;margin-bottom:1rem;opacity:0.3;"></i>
                    <div style="margin-left:1rem;">
                        <h4>Ready for Analysis</h4>
                        <p>Requires successful Mortality Analytics.</p>
                    </div>
                </div>`;
            document.getElementById('demography').innerHTML = `
                <div style="display:flex;align-items:center;justify-content:center;height:400px;color:var(--text-muted);">
                    <i class="fas fa-users" style="font-size:3rem;margin-bottom:1rem;opacity:0.3;"></i>
                    <div style="margin-left:1rem;">
                        <h4>Ready for Analysis</h4>
                        <p>Requires successful Mortality Analytics.</p>
                    </div>
                </div>`;
        }
        // --- Обновляем таблицу сводки ---
        const summaryPromises = countries.map(country => 
            fetchWithErrorHandling(`/covid/summary?country=${encodeURIComponent(country)}&date_from=${startDate}&date_to=${endDate}&case_type=${caseType}`)
                .catch(() => null)
        );
        const summaryResults = await Promise.allSettled(summaryPromises);
        const summaryRows = summaryResults
            .filter(result => result.status === 'fulfilled' && result.value)
            .map(result => result.value);
        if (summaryRows.length > 0) {
            let tableHTML = `
                <div style="overflow-x: auto;">
                    <table class="table">
                        <thead>
                            <tr>
            `;
            const headers = Object.keys(summaryRows[0]);
            const headerIcons = {
                country: '🌍',
                case_type: '📊',
                from: '📅',
                to: '📅',
                value: '📈'
            };
            headers.forEach(header => {
                const icon = headerIcons[header] || '📋';
                tableHTML += `<th>${icon} ${header.replace('_', ' ').toUpperCase()}</th>`;
            });
            tableHTML += '</tr></thead><tbody>';
            summaryRows.forEach((row, index) => {
                tableHTML += `<tr style="animation: slideIn 0.5s ease-out ${index * 0.1}s both;">`;
                headers.forEach(header => {
                    let value = row[header];
                    if (header === 'value' && typeof value === 'number') {
                        value = value.toLocaleString();
                    }
                    if (header === 'from' || header === 'to') {
                         value = value || 'N/A';
                    }
                    tableHTML += `<td>${value}</td>`;
                });
                tableHTML += '</tr>';
            });
            tableHTML += '</tbody></table></div>';
            document.getElementById('summary-table').innerHTML = tableHTML;
        } else {
            document.getElementById('summary-table').innerHTML = `
                <div style="text-align: center; padding: 3rem; color: var(--text-muted);">
                    <i class="fas fa-exclamation-triangle" style="font-size: 3rem; margin-bottom: 1rem; color: var(--warning);"></i>
                    <p>No summary data available for the selected parameters.</p>
                </div>
            `;
        }
    } catch (error) {
        console.error('Global error updating charts:', error);
        const anyChartHasError = chartIds.some(id => {
            const el = document.getElementById(id).querySelector('.fa-exclamation-circle, .fa-exclamation-triangle');
            return el !== null;
        });
        if (!anyChartHasError) {
            document.getElementById('daily-deaths').innerHTML = `
                <div style="display: flex; align-items: center; justify-content: center; height: 400px; color: var(--error);">
                    <i class="fas fa-exclamation-circle" style="font-size: 3rem; margin-right: 1rem;"></i>
                    <div>
                        <h4>Unable to load data</h4>
                        <p>Please check your connection and try again. (${error.message})</p>
                    </div>
                </div>
            `;
        }
    } finally {
        isLoading = false;
        button.innerHTML = originalText;
        button.disabled = false;
    }
}

async function addAnnotation() {
    const author = document.getElementById('author').value.trim();
    const text = document.getElementById('comment').value.trim();
    if (!author || !text) {
        const authorField = document.getElementById('author');
        const commentField = document.getElementById('comment');
        if (!author) {
            authorField.style.borderColor = 'var(--error)';
            authorField.style.boxShadow = '0 0 0 3px rgba(239, 68, 68, 0.1)';
            setTimeout(() => {
                authorField.style.borderColor = '';
                authorField.style.boxShadow = '';
            }, 3000);
        }
        if (!text) {
            commentField.style.borderColor = 'var(--error)';
            commentField.style.boxShadow = '0 0 0 3px rgba(239, 68, 68, 0.1)';
            setTimeout(() => {
                commentField.style.borderColor = '';
                commentField.style.boxShadow = '';
            }, 3000);
        }
        return;
    }
    const button = document.getElementById('add-annotation-btn');
    const originalText = button.innerHTML;
    button.innerHTML = '<div class="loading"></div> Adding...';
    button.disabled = true;
    try {
        const response = await fetch('/annotations', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                dashboard_id: 'covid_dashboard',
                author: author,
                text: text,
                tags: []
            })
        });
        if (response.ok) {
            document.getElementById('author').value = '';
            document.getElementById('comment').value = '';
            button.innerHTML = '<i class="fas fa-check"></i> Added!';
            button.style.background = 'linear-gradient(135deg, var(--success), #22c55e)';
            setTimeout(() => {
                button.innerHTML = originalText;
                button.style.background = '';
            }, 2000);
            loadAnnotations();
        } else {
             throw new Error(`Server error: ${response.status}`);
        }
    } catch (e) {
        console.error('Error adding annotation:', e);
        button.innerHTML = '<i class="fas fa-exclamation"></i> Error';
        button.style.background = 'linear-gradient(135deg, var(--error), #f87171)';
        setTimeout(() => {
            button.innerHTML = originalText;
            button.style.background = '';
        }, 2000);
    } finally {
        button.disabled = false;
    }
}

async function loadAnnotations() {
    try {
        const data = await fetchWithErrorHandling('/annotations');
        const container = document.getElementById('annotations-list');
        if (data.items && data.items.length > 0) {
            let html = '';
            data.items.forEach((item, index) => {
                const date = new Date(item.created_at);
                const timeAgo = getTimeAgo(date);
                html += `
                    <div class="annotation-item" style="animation: slideIn 0.5s ease-out ${index * 0.1}s both;">
                        <div class="annotation-header">
                            <div class="annotation-author">
                                <i class="fas fa-user-circle"></i> ${item.author}
                            </div>
                            <div class="annotation-date">
                                <i class="fas fa-clock"></i> ${timeAgo}
                            </div>
                        </div>
                        <div class="annotation-text">${item.text}</div>
                    </div>
                `;
            });
            container.innerHTML = html;
        } else {
            container.innerHTML = `
                <div style="text-align: center; padding: 2rem; color: var(--text-muted);">
                    <i class="fas fa-sticky-note" style="font-size: 2rem; margin-bottom: 1rem; opacity: 0.3;"></i>
                    <p>No annotations yet. Be the first to share an insight!</p>
                </div>
            `;
        }
    } catch (e) {
        console.error('Error loading annotations:', e);
    }
}

function getTimeAgo(date) {
    const now = new Date();
    const seconds = Math.floor((now - date) / 1000);
    if (seconds < 60) return 'just now';
    if (seconds < 3600) return `${Math.floor(seconds / 60)}m ago`;
    if (seconds < 86400) return `${Math.floor(seconds / 3600)}h ago`;
    return `${Math.floor(seconds / 86400)}d ago`;
}

// --- НОВОЕ: Функция для прогнозирования ---
async function predictInfections() {
    // Получаем выбранную страну
    const selectedOptions = document.getElementById('countries').selectedOptions;
    let countries = Array.from(selectedOptions).map(o => o.value);
    let country = countries[0]; // Берем первую выбранную страну

    if (!country) {
        alert("Please select at least one country first.");
        return;
    }

    const daysAhead = 7; // Количество дней для прогноза
    const button = document.getElementById('predict-btn'); // Кнопка
    const originalText = button.innerHTML;
    const originalWidth = button.style.width; // Сохраняем ширину для плавности

    // Блоки для вывода результата
    const card = document.getElementById('prediction-card'); // Новый скрытый блок
    const outputDiv = document.getElementById('prediction-output'); // Внутри него

    // Меняем кнопку на "Loading..."
    button.innerHTML = '<div class="loading"></div> Predicting...';
    button.disabled = true;
    button.style.width = originalWidth; // Фиксируем ширину

    try {
        // Вызываем API
        const response = await fetch(`/analytics/predict-infections?country=${encodeURIComponent(country)}&days_ahead=${daysAhead}`);
        
        if (!response.ok) {
            const errorData = await response.json().catch(() => ({}));
            throw new Error(errorData.detail || `HTTP error! status: ${response.status}`);
        }
        
        const data = await response.json();

        // --- Формируем HTML для отображения ---
        let html = `<h3 style="text-align:center; margin-bottom: 1rem;"><i class="fas fa-chart-line"></i> Infection Forecast for ${data.country}</h3>`;
        html += `<p style="text-align:center; color: var(--text-muted); margin-bottom: 1.5rem;">Model: ${data.model} | Last Observed: ${data.last_observed_date} (${data.last_observed_value.toLocaleString()} total cases)</p>`;
        html += `<div style="overflow-x: auto;"><table class="table">`;
        html += `<thead><tr><th>Date</th><th>Predicted New Cases</th><th>Confidence Interval</th></tr></thead><tbody>`;

        data.predictions.forEach(p => {
            html += `<tr>
                        <td>${p.date}</td>
                        <td>${p.predicted_cases.toLocaleString()}</td>
                        <td>${p.confidence_lower.toLocaleString()} - ${p.confidence_upper.toLocaleString()}</td>
                     </tr>`;
        });

        html += `</tbody></table></div>`;

        // Выводим результат в новый блок
        outputDiv.innerHTML = html;

        // Делаем блок видимым
        card.style.display = 'block';

        // Прокручиваем страницу к блоку прогноза
        card.scrollIntoView({ behavior: 'smooth', block: 'start' });

    } catch (error) {
        console.error("Error predicting infections:", error);
        // Показываем ошибку в блоке вывода
        outputDiv.innerHTML = `
            <div style="display: flex; align-items: center; justify-content: center; height: 100%; flex-direction: column; color: var(--error);">
                <i class="fas fa-exclamation-triangle" style="font-size: 3rem; margin-bottom: 1rem;"></i>
                <h4>Prediction Failed</h4>
                <p>${error.message}</p>
                <p style="font-size: 0.9rem; margin-top: 1rem;">Please try again or check the server logs.</p>
            </div>
        `;
        card.style.display = 'block'; // Показываем блок даже при ошибке
    } finally {
        // Возвращаем кнопку в исходное состояние
        button.innerHTML = originalText;
        button.disabled = false;
    }
}

// Горячие клавиши
document.addEventListener('keydown', function(e) {
    if (e.ctrlKey || e.metaKey) {
        switch(e.key) {
            case 'Enter':
                e.preventDefault();
                if (!isLoading) {
                    updateCharts();
                }
                break;
            case '/':
                e.preventDefault();
                document.getElementById('comment').focus();
                break;
        }
    }
});

// Инициализация приложения
async function initialize() {
     document.querySelectorAll('.country-btn').forEach(btn => {
        btn.addEventListener('click', () => {
            const group = btn.getAttribute('data-group');
            selectCountryGroup(group);
        });
    });
    document.getElementById('update-btn').addEventListener('click', updateCharts);
    document.getElementById('add-annotation-btn').addEventListener('click', addAnnotation);
    // --- Назначаем обработчик для кнопки прогноза ---
    document.getElementById('predict-btn').addEventListener('click', predictInfections);
    setTimeout(checkHealth, 100);
    setTimeout(loadAnnotations, 300);
    setTimeout(() => {
        selectCountryGroup('europe');
        updateSelectVisual();
    }, 500);
    setInterval(checkHealth, 30000);
    setInterval(loadAnnotations, 45000);
}

document.addEventListener('DOMContentLoaded', function() {
    document.body.style.opacity = '0';
    setTimeout(() => {
        document.body.style.transition = 'opacity 0.5s ease-in-out';
        document.body.style.opacity = '1';
    }, 100);
    initialize();
});

window.addEventListener('resize', function() {
    setTimeout(() => {
        try {
            const chartIds = ['daily-deaths', 'analytics', 'infection-rate', 'demography'];
            chartIds.forEach(id => {
                 const chartDiv = document.getElementById(id);
                 if (chartDiv && window.Plotly) {
                    Plotly.Plots.resize(chartDiv);
                 }
            });
        } catch (e) {
            console.log('Resize handling error:', e);
        }
    }, 300);
});