/**
 * Spotify Lifecycle Dashboard - Main Application
 * 
 * Architecture:
 * - Fetches pre-computed dashboard_data.json from S3
 * - Zero live queries, zero backend calls
 * - Renders charts and lists using Chart.js
 * - Handles loading, error, and empty states
 * 
 * Cost: Zero compute cost (static site, pre-computed data)
 */

// ==========================
// Configuration
// ==========================

const CONFIG = {
    // Use relative URL to avoid CORS issues
    // Data file has Cache-Control: max-age=300 (5 min) for nightly updates
    // Static assets (HTML/JS/CSS) cached longer (invalidate on deploy)
    DATA_URL: 'dashboard_data.json',

    // Chart colors (Spotify theme)
    COLORS: {
        primary: '#1db954',
        primaryDark: '#1ed760',
        background: '#121212',
        text: '#ffffff',
        textSecondary: '#b3b3b3',
        grid: '#282828'
    },

    // Retry configuration
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
// State Management
// ==========================

let dashboardData = null;
let dailyTrendChart = null;
let hourlyChart = null;
let activeWindowKey = null;

// ==========================
// Initialization
// ==========================

document.addEventListener('DOMContentLoaded', async () => {
    console.log('Dashboard initializing...');
    await loadDashboard();
});

// ==========================
// Data Loading
// ==========================

async function loadDashboard() {
    showLoadingState();

    try {
        dashboardData = await fetchDashboardData();

        if (!dashboardData || !dashboardData.metadata) {
            throw new Error('Invalid dashboard data format');
        }

        // Check if data is empty (no plays)
        if (dashboardData.metadata.total_play_count === 0) {
            showEmptyState();
            return;
        }

        initWindowSelector();
        renderDashboard(activeWindowKey);
        showMainContent();

        console.log('Dashboard loaded successfully');
    } catch (error) {
        console.error('Failed to load dashboard:', error);
        showErrorState(error.message);
    }
}

async function fetchDashboardData(retries = 0) {
    try {
        // Add cache-busting timestamp to force fresh data
        // Rounds to nearest 5 minutes to allow some browser caching
        const cacheBuster = Math.floor(Date.now() / (5 * 60 * 1000));
        const url = `${CONFIG.DATA_URL}?t=${cacheBuster}`;

        const response = await fetch(url, {
            cache: 'no-cache',  // Always revalidate with server
            headers: {
                'Cache-Control': 'no-cache, no-store, must-revalidate',
                'Pragma': 'no-cache'
            }
        });

        if (!response.ok) {
            throw new Error(`HTTP ${response.status}: ${response.statusText}`);
        }

        const data = await response.json();
        return data;
    } catch (error) {
        if (retries < CONFIG.MAX_RETRIES) {
            console.warn(`Fetch failed (attempt ${retries + 1}/${CONFIG.MAX_RETRIES + 1}), retrying...`);
            await sleep(CONFIG.RETRY_DELAY_MS);
            return fetchDashboardData(retries + 1);
        }
        throw error;
    }
}

// ==========================
// Rendering
// ==========================

function renderDashboard(windowKey) {
    const windowData = getWindowData(windowKey);
    const fallbackWindow = dashboardData?.windows
        ? dashboardData.windows[dashboardData.metadata?.default_window] ||
        dashboardData.windows[Object.keys(dashboardData.windows)[0]]
        : null;

    const effectiveWindow = windowData || fallbackWindow || null;

    renderSummaryCards(effectiveWindow, dashboardData.metadata);
    renderWindowLabel(windowKey || dashboardData?.metadata?.default_window);
    renderLastUpdated(dashboardData.metadata.generated_at);
    renderDailyTrendChart(dashboardData.daily_plays);
    renderHourlyChart(dashboardData.hourly_distribution);
    renderTopTracks(effectiveWindow?.top_tracks || dashboardData.top_tracks || []);
    renderTopArtists(effectiveWindow?.top_artists || dashboardData.top_artists || []);
    renderTopGenres(effectiveWindow?.top_genres || dashboardData.top_genres || []);
}

function initWindowSelector() {
    const control = document.getElementById('window-control');
    const select = document.getElementById('window-select');

    if (!control || !select) return;

    if (!dashboardData?.windows || Object.keys(dashboardData.windows).length === 0) {
        control.style.display = 'none';
        activeWindowKey = null;
        renderWindowLabel(null);
        return;
    }

    control.style.display = 'flex';
    select.innerHTML = '';

    Object.keys(dashboardData.windows).forEach((key) => {
        const option = document.createElement('option');
        option.value = key;
        option.textContent = labelForWindow(key);
        select.appendChild(option);
    });

    const defaultKey = dashboardData.metadata?.default_window;
    const firstKey = Object.keys(dashboardData.windows)[0];
    activeWindowKey = dashboardData.windows[defaultKey] ? defaultKey : firstKey;
    select.value = activeWindowKey;
    renderWindowLabel(activeWindowKey);

    select.onchange = (event) => {
        activeWindowKey = event.target.value;
        renderDashboard(activeWindowKey);
    };
}

function getWindowData(windowKey) {
    if (!dashboardData || !dashboardData.windows) {
        return null;
    }
    if (windowKey && dashboardData.windows[windowKey]) {
        return dashboardData.windows[windowKey];
    }
    return null;
}

function labelForWindow(windowKey) {
    return WINDOW_LABELS[windowKey] || windowKey || 'All Time';
}

function renderWindowLabel(windowKey) {
    const labelEl = document.getElementById('window-active-label');
    if (!labelEl) return;
    labelEl.textContent = labelForWindow(windowKey || dashboardData?.metadata?.default_window);
}

function renderSummaryCards(windowData, metadata) {
    const plays = windowData?.total_play_count ?? metadata?.total_play_count ?? 0;
    const tracks = windowData?.unique_track_count ?? metadata?.unique_track_count ?? 0;
    const artists = windowData?.unique_artist_count ?? metadata?.unique_artist_count ?? 0;
    const genresCount = (windowData?.top_genres || []).length || metadata?.genre_count || 0;

    document.getElementById('total-plays').textContent = formatNumber(plays);
    document.getElementById('unique-tracks').textContent = formatNumber(tracks);
    document.getElementById('unique-artists').textContent = formatNumber(artists);
    document.getElementById('unique-genres').textContent = formatNumber(genresCount);
}

function renderLastUpdated(timestamp) {
    const date = new Date(timestamp);
    const formatted = date.toLocaleString('en-US', {
        year: 'numeric',
        month: 'long',
        day: 'numeric',
        hour: '2-digit',
        minute: '2-digit',
        timeZoneName: 'short'
    });
    document.getElementById('last-updated').textContent = `Last updated: ${formatted}`;
}

function renderDailyTrendChart(dailyPlays) {
    const ctx = document.getElementById('daily-trend-chart').getContext('2d');

    // Destroy existing chart if it exists
    if (dailyTrendChart) {
        dailyTrendChart.destroy();
    }

    // Sort by date ascending
    const sortedData = dailyPlays.sort((a, b) => new Date(a.date) - new Date(b.date));

    const labels = sortedData.map(item => {
        const date = new Date(item.date);
        return date.toLocaleDateString('en-US', { month: 'short', day: 'numeric' });
    });

    const data = sortedData.map(item => item.play_count);

    dailyTrendChart = new Chart(ctx, {
        type: 'line',
        data: {
            labels: labels,
            datasets: [{
                label: 'Plays',
                data: data,
                borderColor: CONFIG.COLORS.primary,
                backgroundColor: `${CONFIG.COLORS.primary}33`,
                borderWidth: 2,
                fill: true,
                tension: 0.4,
                pointRadius: 2,
                pointHoverRadius: 5
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: true,
            plugins: {
                legend: {
                    display: false
                },
                tooltip: {
                    mode: 'index',
                    intersect: false,
                    backgroundColor: CONFIG.COLORS.background,
                    titleColor: CONFIG.COLORS.text,
                    bodyColor: CONFIG.COLORS.text,
                    borderColor: CONFIG.COLORS.primary,
                    borderWidth: 1
                }
            },
            scales: {
                x: {
                    grid: {
                        color: CONFIG.COLORS.grid,
                        drawBorder: false
                    },
                    ticks: {
                        color: CONFIG.COLORS.textSecondary,
                        maxRotation: 45,
                        minRotation: 45
                    }
                },
                y: {
                    beginAtZero: true,
                    grid: {
                        color: CONFIG.COLORS.grid,
                        drawBorder: false
                    },
                    ticks: {
                        color: CONFIG.COLORS.textSecondary,
                        precision: 0
                    }
                }
            },
            interaction: {
                mode: 'nearest',
                axis: 'x',
                intersect: false
            }
        }
    });
}

function renderHourlyChart(hourlyDistribution) {
    const ctx = document.getElementById('hourly-chart').getContext('2d');

    // Destroy existing chart if it exists
    if (hourlyChart) {
        hourlyChart.destroy();
    }

    const labels = hourlyDistribution.map(item => {
        const hour = item.hour;
        return `${hour}:00`;
    });

    const data = hourlyDistribution.map(item => item.play_count);

    hourlyChart = new Chart(ctx, {
        type: 'bar',
        data: {
            labels: labels,
            datasets: [{
                label: 'Plays',
                data: data,
                backgroundColor: CONFIG.COLORS.primary,
                borderWidth: 0,
                borderRadius: 4
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: true,
            plugins: {
                legend: {
                    display: false
                },
                tooltip: {
                    mode: 'index',
                    intersect: false,
                    backgroundColor: CONFIG.COLORS.background,
                    titleColor: CONFIG.COLORS.text,
                    bodyColor: CONFIG.COLORS.text,
                    borderColor: CONFIG.COLORS.primary,
                    borderWidth: 1
                }
            },
            scales: {
                x: {
                    grid: {
                        display: false,
                        drawBorder: false
                    },
                    ticks: {
                        color: CONFIG.COLORS.textSecondary
                    }
                },
                y: {
                    beginAtZero: true,
                    grid: {
                        color: CONFIG.COLORS.grid,
                        drawBorder: false
                    },
                    ticks: {
                        color: CONFIG.COLORS.textSecondary,
                        precision: 0
                    }
                }
            }
        }
    });
}

function renderTopTracks(topTracks) {
    const container = document.getElementById('top-tracks-list');
    container.innerHTML = '';

    topTracks.forEach((track, index) => {
        const item = createListItem(
            index + 1,
            track.track_name || 'Unknown Track',
            track.play_count
        );
        container.appendChild(item);
    });
}

function renderTopArtists(topArtists) {
    const container = document.getElementById('top-artists-list');
    container.innerHTML = '';

    topArtists.forEach((artist, index) => {
        const item = createListItem(
            index + 1,
            artist.artist_name || 'Unknown Artist',
            artist.play_count
        );
        container.appendChild(item);
    });
}

function renderTopGenres(topGenres) {
    const container = document.getElementById('top-genres-list');
    container.innerHTML = '';

    topGenres.forEach((genre, index) => {
        const item = createListItem(
            index + 1,
            genre.genre || 'Unknown Genre',
            genre.play_count
        );
        container.appendChild(item);
    });
}

function createListItem(rank, name, count) {
    const item = document.createElement('div');
    item.className = 'list-item';

    const rankSpan = document.createElement('span');
    rankSpan.className = 'list-item-rank';
    rankSpan.textContent = `#${rank}`;

    const nameSpan = document.createElement('span');
    nameSpan.className = 'list-item-name';
    nameSpan.textContent = name;

    const countSpan = document.createElement('span');
    countSpan.className = 'list-item-count';
    countSpan.textContent = `${formatNumber(count)} plays`;

    item.appendChild(rankSpan);
    item.appendChild(nameSpan);
    item.appendChild(countSpan);

    return item;
}

// ==========================
// UI State Management
// ==========================

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
// Utility Functions
// ==========================

function formatNumber(num) {
    if (num >= 1000000) {
        return (num / 1000000).toFixed(1) + 'M';
    } else if (num >= 1000) {
        return (num / 1000).toFixed(1) + 'K';
    }
    return num.toString();
}

function sleep(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
}

// ==========================
// Error Handling
// ==========================

window.addEventListener('error', (event) => {
    console.error('Global error:', event.error);
});

window.addEventListener('unhandledrejection', (event) => {
    console.error('Unhandled promise rejection:', event.reason);
});
