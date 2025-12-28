# Spotify Lifecycle Dashboard

**Zero-backend-cost static dashboard** for visualizing Spotify listening analytics.

## Overview

This dashboard is a **static HTML/CSS/JS application** that:

- Fetches pre-computed `dashboard_data.json` from S3 (or local file)
- Renders charts and analytics using Chart.js
- Requires **zero backend compute** (all data is pre-aggregated)
- Costs **$0/month to serve** (static site hosting)

## Architecture

```
Nightly Aggregation (Lambda)
    ↓
dashboard_data.json (S3)
    ↓
Static Website (S3 + CloudFront)
    ↓
Browser (Chart.js rendering)
```

**Key Design Principles:**

- **No live queries**: All data is pre-computed nightly
- **No credentials**: Frontend has no access to Spotify API or DynamoDB
- **No backend**: Pure static site (HTML/CSS/JS only)
- **Instant load**: Data is a single JSON file (<100KB)

## Files

```
dashboard/site/
├── index.html           # Main HTML structure
├── styles.css           # Spotify-themed CSS
├── app.js              # JavaScript application logic
├── dashboard_data.json  # Sample data (for local dev)
└── README.md           # This file
```

## Local Development

### Quick Start

1. **Serve locally:**

   ```bash
   cd dashboard/site
   python3 -m http.server 8000
   ```

2. **Open in browser:**

   ```
   http://localhost:8000
   ```

3. **View dashboard:**
   - Uses sample `dashboard_data.json` by default
   - No S3 or AWS credentials required

### File Serving Options

**Option 1: Python HTTP Server (Simple)**

```bash
cd dashboard/site
python3 -m http.server 8000
```

**Option 2: Node.js HTTP Server**

```bash
cd dashboard/site
npx http-server -p 8000
```

**Option 3: VS Code Live Server Extension**

- Install "Live Server" extension
- Right-click `index.html` → "Open with Live Server"

## Configuration

### S3 Data URL

Edit `app.js` to point to your S3 bucket:

```javascript
const CONFIG = {
    // Replace with your S3 bucket URL
    DATA_URL: 'https://your-bucket-name.s3.us-east-1.amazonaws.com/dashboard_data.json',
    
    // Or CloudFront distribution
    DATA_URL: 'https://d1234567890.cloudfront.net/dashboard_data.json'
};
```

### CORS Configuration

Your S3 bucket must allow CORS for browser access:

```json
{
  "CORSRules": [
    {
      "AllowedOrigins": ["*"],
      "AllowedMethods": ["GET"],
      "AllowedHeaders": ["*"],
      "MaxAgeSeconds": 3600
    }
  ]
}
```

## Deployment

### Option 1: S3 Static Website Hosting

1. **Create S3 bucket:**

   ```bash
   aws s3 mb s3://your-dashboard-bucket
   ```

2. **Enable static website hosting:**

   ```bash
   aws s3 website s3://your-dashboard-bucket \
       --index-document index.html \
       --error-document index.html
   ```

3. **Upload files:**

   ```bash
   cd dashboard/site
   aws s3 sync . s3://your-dashboard-bucket \
       --exclude "dashboard_data.json" \
       --exclude "README.md"
   ```

4. **Set bucket policy (public read):**

   ```json
   {
     "Version": "2012-10-17",
     "Statement": [
       {
         "Sid": "PublicReadGetObject",
         "Effect": "Allow",
         "Principal": "*",
         "Action": "s3:GetObject",
         "Resource": "arn:aws:s3:::your-dashboard-bucket/*"
       }
     ]
   }
   ```

5. **Access:**

   ```
   http://your-dashboard-bucket.s3-website-us-east-1.amazonaws.com
   ```

### Option 2: CloudFront + S3 (Recommended)

**Benefits:**

- HTTPS support
- Global CDN (faster loads)
- Custom domain support
- Better security (origin access control)

**Steps:**

1. Create S3 bucket (private)
2. Upload dashboard files
3. Create CloudFront distribution pointing to S3
4. Use CloudFront URL in frontend

**Cost:** ~$0.50/month (50GB transfer)

### Option 3: GitHub Pages (Free)

1. **Create repo:** `your-username/spotify-dashboard`
2. **Push files:**

   ```bash
   cd dashboard/site
   git init
   git add .
   git commit -m "Initial commit"
   git push origin main
   ```

3. **Enable GitHub Pages:** Settings → Pages → Source: main branch
4. **Access:** `https://your-username.github.io/spotify-dashboard`

**Note:** You'll need to host `dashboard_data.json` separately on S3 (GitHub Pages can't access private S3 directly).

## Features

### Summary Cards

- Total plays
- Unique tracks
- Unique artists
- Genre count

### Charts

- **Daily Listening Trend:** Line chart showing plays over last 90 days
- **Hourly Distribution:** Bar chart showing listening by hour of day

### Top Lists

- **Top 50 Tracks:** Ranked by play count
- **Top 50 Artists:** Ranked by play count
- **Top 20 Genres:** Ranked by play count

### Error Handling

- **Loading state:** Spinner while fetching data
- **Error state:** Displayed if fetch fails (with retry button)
- **Empty state:** Displayed if no listening history

## Customization

### Theme Colors

Edit `styles.css` to change color scheme:

```css
:root {
    --primary-color: #1db954;  /* Spotify green */
    --background: #121212;     /* Dark background */
    --surface: #181818;        /* Card background */
    --text-primary: #ffffff;   /* Primary text */
    --text-secondary: #b3b3b3; /* Secondary text */
}
```

### Chart Configuration

Edit `app.js` to customize charts:

```javascript
const CONFIG = {
    COLORS: {
        primary: '#1db954',    // Chart color
        background: '#121212', // Background
        text: '#ffffff',       // Text color
        grid: '#282828'        // Grid lines
    }
};
```

## Data Format

The dashboard expects `dashboard_data.json` in this format:

```json
{
  "version": "1.0.0",
  "metadata": {
    "generated_at": "2025-12-27T10:30:00Z",
    "total_play_count": 2847,
    "unique_track_count": 542,
    "unique_artist_count": 187,
    "genre_count": 34
  },
  "top_tracks": [
    {"track_id": "...", "track_name": "...", "artist_name": "...", "play_count": 89}
  ],
  "top_artists": [
    {"artist_id": "...", "artist_name": "...", "play_count": 234}
  ],
  "top_genres": [
    {"genre": "pop", "play_count": 892}
  ],
  "daily_plays": [
    {"date": "2025-10-01", "play_count": 28}
  ],
  "hourly_distribution": [
    {"hour": 0, "play_count": 12}
  ]
}
```

This format is generated by `src/spotify_lifecycle/pipeline/aggregate.py`.

## Performance

### Load Time Metrics

- **Initial load:** <500ms (including data fetch)
- **Render time:** <100ms (Chart.js rendering)
- **Data size:** ~50KB (typical), ~200KB (max)

### Optimization Techniques

- **Gzip compression:** Reduces JSON size by ~70%
- **Chart.js CDN:** Fast global delivery
- **Minimal dependencies:** No framework overhead
- **Pre-computed data:** Zero backend latency

## Cost Breakdown

**Static Site Hosting (S3 + CloudFront):**

- Storage: $0.023/GB/month × 0.001GB = $0.00002/month
- Requests: $0.005/10k requests × 100/month = $0.00005/month
- Data transfer: $0.09/GB × 0.05GB/month = $0.0045/month

**Total: ~$0.005/month** (effectively free)

**Comparison to alternatives:**

- Heroku Hobby Dyno: $7/month
- AWS EC2 t2.micro: $8.35/month
- Vercel Pro: $20/month

**Savings: 99.9% cheaper**

## Browser Support

**Supported Browsers:**

- Chrome 90+
- Firefox 88+
- Safari 14+
- Edge 90+

**Required Features:**

- ES6 JavaScript
- Fetch API
- CSS Grid
- Canvas (for Chart.js)

## Security

**No Credentials in Frontend:**

- Dashboard has no access to Spotify API
- Dashboard has no access to DynamoDB
- Dashboard cannot modify data
- Dashboard is read-only view of pre-computed data

**Public Data:**

- All data is aggregated and anonymized
- No user tokens or credentials
- Safe to share publicly

## Troubleshooting

### Dashboard shows "Unable to Load Data"

**Possible causes:**

1. `dashboard_data.json` not found
   - **Fix:** Verify S3 URL in `app.js`
   - **Fix:** Run aggregation pipeline to generate file

2. CORS error (S3)
   - **Fix:** Enable CORS on S3 bucket (see Configuration section)

3. Network error
   - **Fix:** Check browser console for details
   - **Fix:** Verify S3 bucket is public or CloudFront is configured

### Charts not rendering

**Possible causes:**

1. Chart.js not loaded
   - **Fix:** Check browser console for CDN errors
   - **Fix:** Verify internet connection

2. Invalid data format
   - **Fix:** Validate `dashboard_data.json` against schema
   - **Fix:** Check browser console for parsing errors

### Empty state displayed (but data exists)

**Possible cause:**

- `metadata.total_play_count` is 0

**Fix:**

- Re-run aggregation pipeline
- Verify ingestion pipeline has captured plays

## Future Enhancements

**Potential additions:**

- [ ] Time range selector (7d, 30d, 90d, all-time)
- [ ] Search/filter for tracks/artists
- [ ] Export data as CSV
- [ ] Playlist links (Spotify embeds)
- [ ] Genre distribution pie chart
- [ ] Listening streaks/badges
- [ ] Year-in-review summary

**Note:** All enhancements must maintain zero-backend-cost principle.

## Links

- **Main Repo:** [Spotify Lifecycle Manager](../../README.md)
- **Architecture Docs:** [copilot/docs/architecture/](../../copilot/docs/architecture/)
- **Cost Analysis:** [copilot/docs/cost/](../../copilot/docs/cost/)

---

**Last Updated:** December 27, 2025  
**Version:** 1.0.0
