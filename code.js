(async () => {
  /* =====================================
     CONFIG
  ====================================== */
  const DATE_CODE = (() => {
    const d = new Date();
    d.setDate(d.getDate() + 1);
    return d.toISOString().slice(0, 10).replace(/-/g, "");
  })();

  const sleep = ms => new Promise(r => setTimeout(r, ms));

  function downloadJSON(obj, filename) {
    const blob = new Blob([JSON.stringify(obj, null, 2)], {
      type: "application/json",
    });
    const a = document.createElement("a");
    a.href = URL.createObjectURL(blob);
    a.download = filename;
    a.click();
    URL.revokeObjectURL(a.href);
  }

  /* =====================================
     VENUES (INLINE — REQUIRED)
  ====================================== */

  const venues = {

  }

  const venueCodes = Object.keys(venues);
  console.log("✅ Venues loaded:", venueCodes.length);

  /* =====================================
     FETCH (REAL CHROME FETCH)
  ====================================== */
  async function fetchVenue(vcode) {
    const url =
      `https://in.bookmyshow.com/api/v2/mobile/showtimes/byvenue` +
      `?venueCode=${vcode}&dateCode=${DATE_CODE}`;

    const res = await fetch(url, {
      credentials: "include",
      headers: {
        "accept": "application/json, text/plain, */*",
        "accept-language": "en-IN,en;q=0.9",
      },
    });

    const txt = await res.text();
    if (!txt.startsWith("{")) throw "Blocked / HTML";
    return JSON.parse(txt);
  }

  /* =====================================
     PARSER
  ====================================== */
  function parsePayload(data) {
    const out = [];
    const sd = data.ShowDetails || [];
    if (!sd.length) return out;

    const venue = sd[0].Venues || {};
    const venueName = venue.VenueName || "";
    const venueAdd = venue.VenueAdd || "";
    const chain = venue.VenueCompName || "Unknown";

    for (const ev of sd[0].Event || []) {
      const title = ev.EventTitle || "Unknown";

      for (const ch of ev.ChildEvents || []) {
        const dim = ch.EventDimension || "UNKNOWN";
        const lang = ch.EventLanguage || "UNKNOWN";

        for (const sh of ch.ShowTimes || []) {
          if (sh.ShowDateCode !== DATE_CODE) continue;

          let total = 0, sold = 0, avail = 0, gross = 0;

          for (const cat of sh.Categories || []) {
            const seats = +cat.MaxSeats || 0;
            const free = +cat.SeatsAvail || 0;
            const price = +cat.CurPrice || 0;
            total += seats;
            avail += free;
            sold += seats - free;
            gross += (seats - free) * price;
          }

          out.push({
            movie: title,
            venue: venueName,
            address: venueAdd,
            language: lang,
            dimension: dim,
            chain,
            time: sh.ShowTime || "",
            audi: sh.Attributes || "",
            session_id: String(sh.SessionId || ""),
            totalSeats: total,
            available: avail,
            sold,
            gross: +gross.toFixed(2),
          });
        }
      }
    }
    return out;
  }

  /* =====================================
     DEDUPE
  ====================================== */
  function dedupe(rows) {
    const seen = new Set();
    return rows.filter(r => {
      const k = `${r.venue}|${r.time}|${r.session_id}|${r.audi}`;
      if (seen.has(k)) return false;
      seen.add(k);
      return true;
    });
  }

  /* =====================================
     MAIN
  ====================================== */
  console.log("🚀 BMS  SCRIPT STARTED");

  let allRows = [];

  for (let i = 0; i < venueCodes.length; i++) {
    const vcode = venueCodes[i];
    console.log(`[${i + 1}/${venueCodes.length}]`, vcode);

    try {
      const raw = await fetchVenue(vcode);
      const rows = parsePayload(raw);

      for (const r of rows) {
        r.city = venues[vcode]?.City || "Unknown";
        r.state = venues[vcode]?.State || "Unknown";
        r.source = "BMS";
        r.date = DATE_CODE;
      }

      allRows.push(...rows);
    } catch (e) {
      console.warn("❌ Failed:", vcode, e);
    }

    await sleep(700);
  }

  const detailed = dedupe(allRows);

  downloadJSON(detailed, `detailed_${DATE_CODE}.json`);
  console.log("✅ DONE | Shows:", detailed.length);
})();
