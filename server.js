import express from 'express';
import fetch from 'node-fetch';
import * as cheerio from 'cheerio';  // <-- Fix here!
import cors from 'cors';

const app = express();
app.use(cors());

// ================= NSE SCRAPER =================
async function getNSECirculars() {
  const url = 'https://www.nseindia.com/market-data/circulars';
  const res = await fetch(url, { headers: { 'User-Agent': 'Mozilla/5.0' } });
  const html = await res.text();
  const $ = cheerio.load(html);
  let circulars = [];
  $('.circulars-list li').each((_, el) => {
    const title = $(el).find('a').text().trim();
    const link = $(el).find('a').attr('href');
    const date = $(el).find('.date').text().trim();
    circulars.push({
      exchange: 'NSE',
      title,
      link: link?.startsWith('http') ? link : `https://www.nseindia.com${link}`,
      date,
    });
  });
  return circulars;
}

// ================= BSE SCRAPER =================
async function getBSECirculars() {
  const url = 'https://www.bseindia.com/markets/MarketInfo/Notices.aspx';
  const res = await fetch(url, { headers: { 'User-Agent': 'Mozilla/5.0' } });
  const html = await res.text();
  const $ = cheerio.load(html);
  let circulars = [];
  $('table#ctl00_ContentPlaceHolder1_grdData tr').each((i, el) => {
    if (i === 0) return; // skip header
    const tds = $(el).find('td');
    if (tds.length >= 3) {
      const date = $(tds[0]).text().trim();
      const title = $(tds[1]).text().trim();
      const link = $(tds[1]).find('a').attr('href');
      circulars.push({
        exchange: 'BSE',
        title,
        link: link?.startsWith('http') ? link : `https://www.bseindia.com${link}`,
        date,
      });
    }
  });
  return circulars;
}

// ================= MCX SCRAPER =================
async function getMCXCirculars() {
  const url = 'https://www.mcxindia.com/market-data/circulars';
  const res = await fetch(url, { headers: { 'User-Agent': 'Mozilla/5.0' } });
  const html = await res.text();
  const $ = cheerio.load(html);
  let circulars = [];
  $('.circular-list li').each((_, el) => {
    const title = $(el).find('a').text().trim();
    const link = $(el).find('a').attr('href');
    const date = $(el).find('.date').text().trim();
    circulars.push({
      exchange: 'MCX',
      title,
      link: link?.startsWith('http') ? link : `https://www.mcxindia.com${link}`,
      date,
    });
  });
  return circulars;
}

// ================= COMBINED API =================
app.get('/api/all', async (req, res) => {
  try {
    const [nse, bse, mcx] = await Promise.all([
      getNSECirculars(),
      getBSECirculars(),
      getMCXCirculars()
    ]);
    const allData = [...nse, ...bse, ...mcx];

    // Sort by date (optional: handle date parsing for DD/MM/YYYY)
    allData.sort((a, b) => {
      // Parse DD/MM/YYYY
      const parseDate = str => {
        const [d, m, y] = str.split('/').map(Number);
        return new Date(y, m - 1, d);
      };
      return parseDate(b.date) - parseDate(a.date);
    });

    res.json(allData);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.listen(3000, () => console.log('Server running on http://localhost:3000'));