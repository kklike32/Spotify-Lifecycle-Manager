/**
 * Spotify Lifecycle Dashboard - refreshed UI
 * Static site rendering pre-computed listening insights.
 */

// ==========================
// Configuration
// ==========================

const CONFIG = {
    DATA_URL: 'dashboard_data.json',
    LOCAL_DATA_URL: 'https://d25spyc5nz22ju.cloudfront.net/dashboard_data.json',
    USE_REMOTE_DATA_ON_LOCALHOST: false,
    COLORS: {
        accent: '#0f766e',
        accentSoft: 'rgba(15, 118, 110, 0.12)',
        text: '#0f172a',
        textMuted: '#6b7280',
        grid: '#e3e8f1',
        surface: '#ffffff'
    },
    DEBUG_CHARTS: false,
    MAX_RETRIES: 3,
    RETRY_DELAY_MS: 2000
};

const WINDOW_LABELS = {
    all_time: 'All Time',
    year_to_date: 'Year to Date',
    last_30_days: 'Last 30 Days',
    last_7_days: 'Last 7 Days'
};

// ==========================
// State
// ==========================

const state = {
    data: null,
    activeWindow: null,
    filters: {
        search: '',
        sortKey: 'play_count',
        sortDirection: 'desc'
    }
};

let dailyTrendChart = null;
let hourlyChart = null;

// ==========================
// Init
// ==========================

document.addEventListener('DOMContentLoaded', initDashboard);

async function initDashboard() {
    showLoadingState();

    try {
        state.data = await fetchData();

        if (!state.data?.metadata) {
            throw new Error('Invalid dashboard data format');
        }

        if (state.data.metadata.total_play_count === 0) {
            showEmptyState();
            return;
        }

        state.activeWindow = resolveWindowKey(state.data);
        renderSummary();
        renderFilters();
        renderTable();
        renderSupportingLists();
        renderCharts();
        showMainContent();
    } catch (error) {
        console.error('Failed to load dashboard:', error);
        showErrorState(error.message);
    }
}

// ==========================
// Data loading
// ==========================

async function fetchData(retries = 0) {
    const dataUrl = resolveDataUrl();
    const cacheBuster = Math.floor(Date.now() / (5 * 60 * 1000));
    const url = `${dataUrl}?t=${cacheBuster}`;
    const isSameOrigin = isSameOriginUrl(dataUrl);
    const requestOptions = isSameOrigin
        ? {
            cache: 'no-cache',
            headers: {
                'Cache-Control': 'no-cache, no-store, must-revalidate',
                Pragma: 'no-cache'
            }
        }
        : { cache: 'no-cache' };

    try {
        const response = await fetch(url, requestOptions);

        if (!response.ok) {
            throw new Error(`HTTP ${response.status}: ${response.statusText}`);
        }

        return await response.json();
    } catch (error) {
        if (retries < CONFIG.MAX_RETRIES) {
            await sleep(CONFIG.RETRY_DELAY_MS);
            return fetchData(retries + 1);
        }
        throw error;
    }
}

// ==========================
// Rendering
// ==========================

function renderSummary() {
    const windowData = getWindowData();
    const metadata = state.data.metadata;

    const plays = windowData?.total_play_count ?? metadata?.total_play_count ?? 0;
    const tracks = windowData?.unique_track_count ?? metadata?.unique_track_count ?? 0;
    const artists = windowData?.unique_artist_count ?? metadata?.unique_artist_count ?? 0;
    const genresCount = (windowData?.top_genres || []).length || metadata?.genre_count || 0;

    document.getElementById('total-plays').textContent = formatNumber(plays);
    document.getElementById('unique-tracks').textContent = formatNumber(tracks);
    document.getElementById('unique-artists').textContent = formatNumber(artists);
    document.getElementById('unique-genres').textContent = formatNumber(genresCount);

    renderWindowLabel(state.activeWindow);
    renderLastUpdated(metadata?.generated_at);
}

function renderFilters() {
    const hasWindows = state.data?.windows && Object.keys(state.data.windows).length > 0;
    const control = document.getElementById('window-control');
    const select = document.getElementById('window-select');
    const searchInput = document.getElementById('search-input');
    const sortButtons = document.querySelectorAll('#sort-buttons .pill-btn');

    if (hasWindows && control && select) {
        control.style.display = 'block';
        select.innerHTML = '';

        Object.keys(state.data.windows).forEach((key) => {
            const option = document.createElement('option');
            option.value = key;
            option.textContent = labelForWindow(key);
            select.appendChild(option);
        });

        select.value = state.activeWindow;
        select.onchange = (event) => {
            state.activeWindow = event.target.value;
            renderSummary();
            renderTable();
            renderSupportingLists();
            renderCharts();
        };
    } else if (control) {
        control.style.display = 'none';
    }

    if (searchInput) {
        searchInput.value = state.filters.search;
        searchInput.addEventListener('input', (event) => {
            state.filters.search = event.target.value;
            renderTable();
        });
    }

    sortButtons.forEach((button) => {
        button.dataset.label = button.dataset.label || button.textContent.trim();
        button.addEventListener('click', () => {
            const sortKey = button.dataset.sort;
            const defaultDirection = button.dataset.direction || 'asc';

            if (state.filters.sortKey === sortKey) {
                state.filters.sortDirection = state.filters.sortDirection === 'asc' ? 'desc' : 'asc';
            } else {
                state.filters.sortKey = sortKey;
                state.filters.sortDirection = defaultDirection;
            }

            updateSortButtons();
            renderTable();
        });
    });

    updateSortButtons();
}

function renderTable() {
    const tableBody = document.querySelector('#tracks-table tbody');
    const resultCount = document.getElementById('result-count');

    if (!tableBody) return;

    const baseTracks = getActiveTracks();
    const tracks = applyFiltersAndSort(baseTracks);

    tableBody.innerHTML = '';

    if (tracks.length === 0) {
        const row = document.createElement('tr');
        const cell = document.createElement('td');
        cell.colSpan = 5;
        cell.textContent = 'No tracks match your filters.';
        cell.className = 'muted';
        row.appendChild(cell);
        tableBody.appendChild(row);
    } else {
        tracks.forEach((track, index) => {
            tableBody.appendChild(buildTrackRow(track, index));
        });
    }

    if (resultCount) {
        resultCount.textContent = `Showing ${tracks.length} of ${baseTracks.length} tracks`;
    }
}

function renderSupportingLists() {
    const windowData = getWindowData();
    renderTopArtists(windowData?.top_artists || state.data.top_artists || []);
    renderTopGenres(windowData?.top_genres || state.data.top_genres || []);
}

function renderCharts() {
    const dailyPlays = getFilteredDailyPlays();
    updateDailyChartSubtitle();
    logDailyChartDiagnostics(dailyPlays, state.activeWindow, getWindowData());
    renderDailyTrendChart(dailyPlays);
    renderHourlyChart(state.data.hourly_distribution || []);
}

function renderDailyTrendChart(dailyPlays) {
    const canvas = document.getElementById('daily-trend-chart');
    if (!canvas || !Array.isArray(dailyPlays)) return;

    const ctx = canvas.getContext('2d');

    if (dailyTrendChart) {
        dailyTrendChart.destroy();
    }

    const sortedData = [...dailyPlays].sort((a, b) => a.date.localeCompare(b.date));

    const labels = sortedData.map((item) =>
        parseLocalDate(item.date).toLocaleDateString('en-US', { month: 'short', day: 'numeric' })
    );

    const data = sortedData.map((item) => item.play_count);

    dailyTrendChart = new Chart(ctx, {
        type: 'line',
        data: {
            labels,
            datasets: [
                {
                    label: 'Plays',
                    data,
                    borderColor: CONFIG.COLORS.accent,
                    backgroundColor: CONFIG.COLORS.accentSoft,
                    borderWidth: 2,
                    fill: true,
                    tension: 0.35,
                    pointRadius: 2,
                    pointHoverRadius: 5
                }
            ]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: {
                legend: { display: false },
                tooltip: {
                    mode: 'index',
                    intersect: false,
                    backgroundColor: '#0f172a',
                    titleColor: '#ffffff',
                    bodyColor: '#ffffff',
                    borderColor: CONFIG.COLORS.accent,
                    borderWidth: 1
                }
            },
            scales: {
                x: {
                    grid: {
                        color: '#d7dee9',
                        drawBorder: false
                    },
                    ticks: {
                        color: '#334155',
                        maxRotation: 0,
                        minRotation: 0,
                        maxTicksLimit: 8,
                        autoSkip: true,
                        font: { size: 13 }
                    }
                },
                y: {
                    beginAtZero: true,
                    grid: {
                        color: '#d7dee9',
                        drawBorder: false
                    },
                    ticks: { color: '#334155', precision: 0, font: { size: 13 } }
                }
            },
            interaction: { mode: 'nearest', axis: 'x', intersect: false }
        }
    });
}

function renderHourlyChart(hourlyDistribution) {
    const canvas = document.getElementById('hourly-chart');
    if (!canvas || !Array.isArray(hourlyDistribution)) return;

    const ctx = canvas.getContext('2d');

    if (hourlyChart) {
        hourlyChart.destroy();
    }

    const labels = hourlyDistribution.map((item) => `${item.hour}:00`);
    const data = hourlyDistribution.map((item) => item.play_count);

    hourlyChart = new Chart(ctx, {
        type: 'bar',
        data: {
            labels,
            datasets: [
                {
                    label: 'Plays',
                    data,
                    backgroundColor: CONFIG.COLORS.accent,
                    borderWidth: 0,
                    borderRadius: 6
                }
            ]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: {
                legend: { display: false },
                tooltip: {
                    mode: 'index',
                    intersect: false,
                    backgroundColor: '#0f172a',
                    titleColor: '#ffffff',
                    bodyColor: '#ffffff',
                    borderColor: CONFIG.COLORS.accent,
                    borderWidth: 1
                }
            },
            scales: {
                x: {
                    grid: { display: false, drawBorder: false },
                    ticks: { color: '#334155', font: { size: 13 }, maxRotation: 0, minRotation: 0 }
                },
                y: {
                    beginAtZero: true,
                    grid: {
                        color: '#d7dee9',
                        drawBorder: false
                    },
                    ticks: { color: '#334155', precision: 0, font: { size: 13 } }
                }
            }
        }
    });
}

// ==========================
// Data helpers
// ==========================

function resolveWindowKey(data) {
    const windows = data?.windows;
    if (!windows || Object.keys(windows).length === 0) return null;

    const defaultKey = data.metadata?.default_window;
    if (defaultKey && windows[defaultKey]) return defaultKey;

    return Object.keys(windows)[0];
}

function getWindowData() {
    if (!state.data?.windows || !state.activeWindow) return null;
    return state.data.windows[state.activeWindow] || null;
}

function getFilteredDailyPlays() {
    const dailyPlays = state.data?.daily_plays || [];
    if (!Array.isArray(dailyPlays) || dailyPlays.length === 0) return [];

    const windowData = getWindowData();
    const startIso = windowData?.start;
    const endIso = windowData?.end;

    if (!startIso || !endIso) return dailyPlays;

    const startDate = new Date(startIso);
    const endDate = new Date(endIso);

    if (Number.isNaN(startDate.getTime()) || Number.isNaN(endDate.getTime())) return dailyPlays;

    let rangeStart = toLocalDateStart(startDate);
    let rangeEnd = toLocalDateEnd(endDate);

    if (rangeStart > rangeEnd) {
        const temp = rangeStart;
        rangeStart = rangeEnd;
        rangeEnd = temp;
    }

    return dailyPlays.filter((item) => {
        if (!item?.date) return false;
        const localDate = parseLocalDate(item.date);
        return localDate >= rangeStart && localDate <= rangeEnd;
    });
}

function getActiveTracks() {
    const windowData = getWindowData();
    return windowData?.top_tracks || state.data.top_tracks || [];
}

function applyFiltersAndSort(tracks) {
    const searchTerm = state.filters.search.trim().toLowerCase();
    const enriched = tracks.map((track, index) => ({ ...track, _index: index }));

    const filtered = searchTerm
        ? enriched.filter((track) => {
            const haystack = [
                track.track_name || '',
                track.artist_name || '',
                track.album_name || ''
            ]
                .join(' ')
                .toLowerCase();
            return haystack.includes(searchTerm);
        })
        : enriched;

    const direction = state.filters.sortDirection === 'asc' ? 1 : -1;
    const sortKey = state.filters.sortKey;

    return filtered.sort((a, b) => {
        const aVal = a[sortKey];
        const bVal = b[sortKey];

        if (sortKey === 'play_count') {
            return ((aVal ?? 0) - (bVal ?? 0)) * direction || a._index - b._index;
        }

        const aStr = (aVal || '').toString().toLowerCase();
        const bStr = (bVal || '').toString().toLowerCase();
        const comparison = aStr.localeCompare(bStr);

        return comparison * direction || a._index - b._index;
    });
}

function buildTrackRow(track, index) {
    const row = document.createElement('tr');

    const rankCell = document.createElement('td');
    rankCell.textContent = (track._index ?? index) + 1;
    rankCell.setAttribute('aria-label', `Rank ${(track._index ?? index) + 1}`);

    const titleCell = document.createElement('td');
    titleCell.textContent = track.track_name || 'Unknown Track';
    titleCell.title = track.track_name || 'Unknown Track';
    titleCell.className = 'truncate';

    const artistCell = document.createElement('td');
    artistCell.textContent = track.artist_name || 'Unknown Artist';
    artistCell.title = track.artist_name || 'Unknown Artist';
    artistCell.className = 'truncate';

    const albumCell = document.createElement('td');
    albumCell.textContent = track.album_name || 'Unknown Album';
    albumCell.title = track.album_name || 'Unknown Album';
    albumCell.className = 'truncate';

    const playsCell = document.createElement('td');
    playsCell.className = 'numeric';
    playsCell.textContent = formatNumber(track.play_count ?? 0);

    row.append(rankCell, titleCell, artistCell, albumCell, playsCell);
    return row;
}

function renderTopArtists(artists) {
    const container = document.getElementById('top-artists-list');
    if (!container) return;
    container.innerHTML = '';

    artists.forEach((artist, index) => {
        const row = createListRow(
            index + 1,
            artist.artist_name || 'Unknown Artist',
            `${formatNumber(artist.play_count ?? 0)} plays`
        );
        container.appendChild(row);
    });
}

function renderTopGenres(genres) {
    const container = document.getElementById('top-genres-list');
    if (!container) return;
    container.innerHTML = '';

    genres.forEach((genre, index) => {
        const row = createListRow(
            index + 1,
            genre.genre || 'Unknown Genre',
            `${formatNumber(genre.play_count ?? 0)} plays`
        );
        container.appendChild(row);
    });
}

function createListRow(rank, name, meta) {
    const row = document.createElement('div');
    row.className = 'list-row';

    const rankSpan = document.createElement('span');
    rankSpan.className = 'list-rank';
    rankSpan.textContent = `#${rank}`;

    const nameSpan = document.createElement('span');
    nameSpan.className = 'list-name truncate';
    nameSpan.textContent = name;
    nameSpan.title = name;

    const metaSpan = document.createElement('span');
    metaSpan.className = 'list-count';
    metaSpan.textContent = meta;

    row.append(rankSpan, nameSpan, metaSpan);
    return row;
}

// ==========================
// UI state
// ==========================

function updateSortButtons() {
    const sortButtons = document.querySelectorAll('#sort-buttons .pill-btn');
    sortButtons.forEach((button) => {
        const label = button.dataset.label || '';
        const isActive = button.dataset.sort === state.filters.sortKey;
        button.classList.toggle('active', isActive);
        button.setAttribute('aria-pressed', isActive ? 'true' : 'false');

        if (isActive) {
            const dirIcon = state.filters.sortDirection === 'asc' ? '↑' : '↓';
            button.textContent = `${label} ${dirIcon}`;
        } else {
            button.textContent = label;
        }
    });
}

function renderWindowLabel(windowKey) {
    const labelEl = document.getElementById('window-active-label');
    if (!labelEl) return;
    labelEl.textContent = labelForWindow(windowKey);
}

function renderLastUpdated(timestamp) {
    if (!timestamp) return;
    const date = new Date(timestamp);
    const formatted = date.toLocaleString('en-US', {
        year: 'numeric',
        month: 'long',
        day: 'numeric',
        hour: '2-digit',
        minute: '2-digit',
        timeZoneName: 'short'
    });
    document.getElementById('last-updated').textContent = `Last updated ${formatted}`;
}

function showLoadingState() {
    document.getElementById('loading-state').style.display = 'flex';
    document.getElementById('error-state').style.display = 'none';
    document.getElementById('empty-state').style.display = 'none';
    document.getElementById('main-content').style.display = 'none';
}

function showErrorState(message) {
    document.getElementById('loading-state').style.display = 'none';
    document.getElementById('error-state').style.display = 'flex';
    document.getElementById('empty-state').style.display = 'none';
    document.getElementById('main-content').style.display = 'none';

    if (message) {
        document.getElementById('error-message').textContent = message;
    }
}

function showEmptyState() {
    document.getElementById('loading-state').style.display = 'none';
    document.getElementById('error-state').style.display = 'none';
    document.getElementById('empty-state').style.display = 'flex';
    document.getElementById('main-content').style.display = 'none';
}

function showMainContent() {
    document.getElementById('loading-state').style.display = 'none';
    document.getElementById('error-state').style.display = 'none';
    document.getElementById('empty-state').style.display = 'none';
    document.getElementById('main-content').style.display = 'block';
}

// ==========================
// Utilities
// ==========================

function labelForWindow(windowKey) {
    return WINDOW_LABELS[windowKey] || 'All Time';
}

function resolveDataUrl() {
    if (window.__DASHBOARD_DATA_URL__) return window.__DASHBOARD_DATA_URL__;

    const hostname = window.location.hostname;
    const isLocalhost = hostname === 'localhost' || hostname === '127.0.0.1';

    if (isLocalhost && CONFIG.USE_REMOTE_DATA_ON_LOCALHOST && CONFIG.LOCAL_DATA_URL) {
        return CONFIG.LOCAL_DATA_URL;
    }

    return CONFIG.DATA_URL;
}

function isSameOriginUrl(url) {
    try {
        const resolved = new URL(url, window.location.href);
        return resolved.origin === window.location.origin;
    } catch (error) {
        return true;
    }
}

function formatNumber(num) {
    if (num >= 1000000) {
        return (num / 1000000).toFixed(1) + 'M';
    }
    if (num >= 1000) {
        return (num / 1000).toFixed(1) + 'K';
    }
    return num.toString();
}

function sleep(ms) {
    return new Promise((resolve) => setTimeout(resolve, ms));
}

function parseLocalDate(dateStr) {
    if (!dateStr) return null;
    const [y, m, d] = dateStr.split('-').map(Number);
    if (!y || !m || !d) return null;
    return new Date(y, m - 1, d); // local midnight to avoid UTC shifts
}

function toLocalDateStart(date) {
    return new Date(date.getFullYear(), date.getMonth(), date.getDate());
}

function toLocalDateEnd(date) {
    return new Date(date.getFullYear(), date.getMonth(), date.getDate(), 23, 59, 59, 999);
}

function logDailyChartDiagnostics(dailyPlays, windowKey, windowData) {
    if (!CONFIG.DEBUG_CHARTS) return;

    const startLabel = windowData?.start || 'n/a';
    const endLabel = windowData?.end || 'n/a';

    if (!Array.isArray(dailyPlays) || dailyPlays.length === 0) {
        console.debug(
            `[charts] daily trend (${windowKey || 'unknown'}) window=${startLabel}→${endLabel}: 0 points`
        );
        return;
    }

    const dates = dailyPlays
        .map((item) => item?.date)
        .filter(Boolean)
        .sort();

    const minDate = dates[0] || 'n/a';
    const maxDate = dates[dates.length - 1] || 'n/a';

    console.debug(
        `[charts] daily trend (${windowKey || 'unknown'}) window=${startLabel}→${endLabel}: ${dailyPlays.length} points (${minDate} → ${maxDate})`
    );
}

function updateDailyChartSubtitle() {
    let subtitle = document.getElementById('daily-trend-subtitle');

    if (!subtitle) {
        const canvas = document.getElementById('daily-trend-chart');
        if (!canvas) return;

        const panel = canvas.closest('.panel');
        if (!panel) return;

        subtitle = panel.querySelector('.panel-header .muted');
        if (!subtitle) return;
    }

    subtitle.textContent = labelForWindow(state.activeWindow);
}

// ==========================
// Error handling
// ==========================

window.addEventListener('error', (event) => {
    console.error('Global error:', event.error);
});

window.addEventListener('unhandledrejection', (event) => {
    console.error('Unhandled promise rejection:', event.reason);
});
