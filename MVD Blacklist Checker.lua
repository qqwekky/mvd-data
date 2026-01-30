script_name("MVD Blacklist Checker")
script_author("qqwekky (modified)")
script_version("v1.15-ready-once")

require "lib.moonloader"
local sampev = require "lib.samp.events"

-- URLs
local url_csv = "https://raw.githubusercontent.com/qqwekky/mvd-data/main/mvd_blacklist.csv"
local url_self = "https://raw.githubusercontent.com/qqwekky/mvd-data/refs/heads/main/MVD%20Blacklist%20Checker.lua"

-- paths
local path_csv = getWorkingDirectory() .. "\\mvd_blacklist.csv"
local logpath = getWorkingDirectory() .. "\\mvd_error.log"
local path_self = getWorkingDirectory() .. "\\MVD Blacklist Checker.lua"

-- update intervals
local UPDATE_INTERVAL_DAYS = 3
local UPDATE_INTERVAL_SECONDS = UPDATE_INTERVAL_DAYS * 24 * 60 * 60

local blacklist = {}
local loaded = false
local rp_enabled = true

-- download state (csv and self)
local dl_csv = { attempt = 0, maxAttempts = 3, nextAttemptTime = 0, inProgress = false, last_status = nil, tempPath = nil }
local dl_self = { attempt = 0, maxAttempts = 3, nextAttemptTime = 0, inProgress = false, last_status = nil, tempPath = nil }

local DEBUG_LOG_LOOKUPS = true
local DEBUG_LOG_PAGING = true
local DEBUG_LOG_INDEXING = true
local DEBUG_LOG_DUMP_MATCHES = true

local ready_announced = false

local function trim(s) if not s then return "" end return s:match("^%s*(.-)%s*$") end

local function writeLog(msg)
    local f = io.open(logpath, "a")
    if f then
        f:write("["..os.date("%Y-%m-%d %H:%M:%S").."] "..tostring(msg).."\n")
        f:close()
    end
end

local function fileExists(p) local f = io.open(p, "rb") if f then f:close() return true end return false end

-- print "Готов к работе" only once
local function announceReadyOnce()
    if not ready_announced then
        sampAddChatMessage("{00FF00}[MVD] Готов к работе", -1)
        ready_announced = true
    end
end

-- read whole file as binary string (returns nil on error)
local function readFileBinary(path)
    local f = io.open(path, "rb")
    if not f then return nil end
    local content = f:read("*all")
    f:close()
    return content
end

-- compare two files byte-by-byte (returns true if equal)
local function filesEqual(a, b)
    local ca = readFileBinary(a)
    local cb = readFileBinary(b)
    if not ca or not cb then return false end
    if #ca ~= #cb then return false end
    return ca == cb
end

-- safely replace dest with src (remove dest first if exists)
local function replaceFile(src, dest)
    if not fileExists(src) then return false, "src_missing" end
    if fileExists(dest) then
        local ok, err = os.remove(dest)
        if not ok then
            return false, "remove_dest_failed: "..tostring(err)
        end
    end
    local ok, err = os.rename(src, dest)
    if not ok then
        -- fallback: copy content
        local content = readFileBinary(src)
        if not content then return false, "read_src_failed" end
        local f = io.open(dest, "wb")
        if not f then return false, "open_dest_failed" end
        f:write(content)
        f:close()
        os.remove(src)
        return true
    end
    return true
end

local function normalize_for_index(name)
    if not name then return "" end
    name = tostring(name)
    name = name:gsub("\239\187\191", "")
    name = name:gsub("\194\160", " ")
    name = name:gsub("[%c%z]", "")
    name = name:gsub('^%s*"', '')
    name = name:gsub('"%s*$', '')
    name = name:match("^%s*(.-)%s*$") or name
    name = name:lower()
    name = name:gsub("_", " ")
    name = name:gsub("[^%w%s]+", " ")
    name = name:gsub("%s+", " ")
    name = name:match("^%s*(.-)%s*$") or name
    return name
end

local function generate_variants(base)
    local out = {}
    if not base or base == "" then return out end
    local n = normalize_for_index(base)
    if n == "" then return out end
    local cand = {
        n,
        n:gsub("%s","_"),
        n:gsub("%s",""),
        (base:lower():gsub("\239\187\191",""):gsub("\194\160"," "):gsub("[%c%z]",""):match("^%s*(.-)%s*$")) or nil,
        (base:lower():gsub("_"," "):gsub("[%c%z]",""):match("^%s*(.-)%s*$")) or nil
    }
    local seen = {}
    for _,v in ipairs(cand) do
        if v and v ~= "" then
            v = trim(v)
            if not seen[v] then seen[v] = true table.insert(out, v) end
        end
    end
    return out
end

-- add to global blacklist (kept)
local function addNick(raw, lvl)
    if not raw then return false end
    raw = tostring(raw)
    raw = raw:gsub("\239\187\191",""):gsub("\194\160"," "):match("^%s*(.-)%s*$") or raw
    if raw == "" then return false end
    local variants = generate_variants(raw)
    for _,v in ipairs(variants) do
        blacklist[v] = lvl or true
        if DEBUG_LOG_INDEXING then writeLog("Indexed variant: '"..tostring(v).."' from raw '"..tostring(raw).."'") end
    end
    return true
end

-- improved splitCSV to handle doubled quotes inside quoted fields
local function splitCSV(line)
    local res, cur, inQuotes = {}, "", false
    local i = 1
    while i <= #line do
        local c = line:sub(i,i)
        if c == '"' then
            if inQuotes and line:sub(i+1,i+1) == '"' then
                cur = cur .. '"'
                i = i + 1
            else
                inQuotes = not inQuotes
            end
        elseif c == ',' and not inQuotes then
            table.insert(res, cur)
            cur = ""
        else
            cur = cur .. c
        end
        i = i + 1
    end
    table.insert(res, cur)
    return res
end

local function looks_like_nick(col)
    if not col then return false end
    local s = tostring(col)
    s = s:gsub("\239\187\191",""):gsub("\194\160"," "):gsub("[%c%z]",""):match("^%s*(.-)%s*$") or ""
    if s == "" then return false end
    s = s:gsub('^"', ''):gsub('"$', '')
    if s:find("[A-Za-z]") or s:find("_") then
        local stripped = s:gsub("%s","")
        if stripped:match("^%d+$") then
            return false
        end
        return true
    end
    return false
end

-- parse CSV at path into a temporary table (doesn't mutate global blacklist)
local function parseCSVToTable(p)
    if not fileExists(p) then return nil end
    local f = io.open(p, "rb")
    if not f then return nil end
    local content = f:read("*all")
    f:close()
    if not content or content == "" then return nil end
    if content:sub(1,3) == "\239\187\191" then content = content:sub(4) end

    local temp = {}
    local row_count = 0
    for line in content:gmatch("[^\r\n]+") do
        row_count = row_count + 1
        local cols = splitCSV(line)
        for i = 1, #cols do
            local col = cols[i]
            if col and trim(col) ~= "" then
                if looks_like_nick(col) then
                    local variants = generate_variants(col)
                    for _,v in ipairs(variants) do
                        temp[v] = 1
                    end
                else
                    if DEBUG_LOG_INDEXING then writeLog("Skipped non-nick column during safe parse: '"..tostring(col).."'") end
                end
            end
        end
    end

    local unique = 0
    for k,_ in pairs(temp) do unique = unique + 1 end

    return { table = temp, rows = row_count, unique = unique }
end

local function applyParsedTable(parsed)
    if not parsed or not parsed.table then return false end
    blacklist = {}
    for k,v in pairs(parsed.table) do
        blacklist[k] = v
    end
    loaded = true
    writeLog("Applied parsed CSV: rows="..tostring(parsed.rows)..", unique_keys="..tostring(parsed.unique))
    return true
end

local function tryLoadFromLocalCSV()
    if not fileExists(path_csv) then return false end
    local parsed = parseCSVToTable(path_csv)
    if not parsed then return false end
    if parsed.unique < 5 then
        writeLog("Local CSV parsed but rejected: unique keys < 5 ("..tostring(parsed.unique)..")")
        return false
    end
    applyParsedTable(parsed)
    writeLog("Loaded blacklist from local file: "..tostring(path_csv))
    return true
end

local function startDownloadGeneric(downloadUrl, destPath, stateTable, label)
    if not downloadUrl then
        writeLog("No URL provided for "..tostring(label)..", skipping download.")
        return
    end
    if stateTable.inProgress then return end
    if stateTable.attempt >= stateTable.maxAttempts and stateTable.nextAttemptTime ~= 0 and os.time() < stateTable.nextAttemptTime then
        return
    end
    stateTable.attempt = stateTable.attempt + 1
    stateTable.inProgress = true
    stateTable.tempPath = destPath .. ".tmp"
    writeLog("Starting download attempt "..tostring(stateTable.attempt).." for "..tostring(label).." url="..tostring(downloadUrl).." -> temp="..tostring(stateTable.tempPath))
    downloadUrlToFile(downloadUrl, stateTable.tempPath, function(id, status)
        stateTable.last_status = status
        stateTable.inProgress = false
        writeLog("download callback for "..tostring(label).." attempt "..tostring(stateTable.attempt).." status="..tostring(status).." temp="..tostring(stateTable.tempPath))
    end)
end

local function scheduleNextAttemptFor(stateTable, label)
    local backoff = 2 ^ (math.max(0, stateTable.attempt - 1))
    stateTable.nextAttemptTime = os.time() + backoff
    writeLog("Scheduling next attempt for "..tostring(label).." in "..tostring(backoff).."s")
end

local history_capture = {
    active = false,
    expecting_dialog = false,
    target = nil,
    display_target = nil,
    pages_visited = 0,
    max_pages = 10,
    results = {},
    _direct_found = nil,
    _roleplay_sent = false,
    _check_timestamp = 0
}

local function resetHistoryCapture()
    history_capture.active = false
    history_capture.expecting_dialog = false
    history_capture.target = nil
    history_capture.display_target = nil
    history_capture.pages_visited = 0
    history_capture.results = {}
    history_capture._direct_found = nil
    history_capture._roleplay_sent = false
    history_capture._check_timestamp = 0
end

local function checkName(raw)
    if not raw then return nil end
    raw = tostring(raw)
    raw = raw:gsub("\239\187\191",""):gsub("\194\160"," "):gsub("[%c%z]",""):match("^%s*(.-)%s*$") or raw
    if raw == "" then return nil end
    local n = normalize_for_index(raw)
    local variants = { n, n and n:gsub("%s","_") or nil, n and n:gsub("%s","") or nil, raw:lower() }
    if DEBUG_LOG_LOOKUPS then
        writeLog("Checking raw: "..tostring(raw))
        writeLog("Normalized: "..tostring(n))
        writeLog("Variants: space='"..tostring(n).."', underscore='"..tostring(n and n:gsub('%s','_') or "").."', nospace='"..tostring(n and n:gsub('%s','') or "").."', rawlower='"..tostring(raw:lower()).."'")
    end
    for _,v in ipairs(variants) do
        if v and v ~= "" then
            if blacklist[v] then
                if DEBUG_LOG_LOOKUPS then writeLog("Match found for variant: "..tostring(v).." -> level="..tostring(blacklist[v])) end
                return blacklist[v], v
            else
                if DEBUG_LOG_LOOKUPS then writeLog("Variant not found in blacklist: "..tostring(v)) end
            end
        end
    end
    if DEBUG_LOG_LOOKUPS then writeLog("No match for: "..tostring(raw)) end
    return nil, nil
end

local function dumpBlacklistMatches(sub, limit)
    if not sub or sub == "" then return end
    local found = {}
    local cnt = 0
    for k,_ in pairs(blacklist) do
        if k:find(sub, 1, true) then
            table.insert(found, k)
            cnt = cnt + 1
            if cnt >= limit then break end
        end
    end
    if #found > 0 then
        writeLog("Blacklist keys containing '"..tostring(sub).."': "..table.concat(found, ", "))
    else
        writeLog("No blacklist keys contain substring '"..tostring(sub).."'.")
    end
end

local function isPageButtonText_server(txt)
    if not txt or txt == "" then return false end
    local s = txt:lower()
    s = s:gsub("%s+", " ")
    if s:find("стр") then return true end
    if s:find(">>") then return true end
    return false
end

local function isCloseButtonText_server(txt)
    if not txt or txt == "" then return false end
    local s = trim(txt):lower()
    if s == "закрыть" then return true end
    return false
end

local function extractNameFromHistoryLine_server(line)
    if not line then return nil end
    local s = trim(line)
    if s == "" then return nil end
    local name = s:match("До%s+%d+%.%d+%.%d+%s+[%-%—–]%s*(.+)")
    if name and name ~= "" then return trim(name) end
    if s:find("_") then
        for token in s:gmatch("[^%s]+") do
            if token:find("_") then return token end
        end
    end
    local maybe = s:match("^[%s]*([%a%p%s]+)$")
    if maybe and maybe ~= "" then
        maybe = trim(maybe)
        if maybe:find("[A-Za-z]") and not (maybe:gsub("%s",""):match("^%d+$")) then
            return maybe
        end
    end
    return nil
end

local function formatNickForChat(param)
    if not param then return "" end
    return tostring(param):gsub("_", " ")
end

local function sendRoleplaySequenceAsync(displayNick, isFound)
    if not rp_enabled then
        if DEBUG_LOG_LOOKUPS then writeLog("RP disabled, skipping roleplay for "..tostring(displayNick)) end
        return
    end

    local nick = formatNickForChat(displayNick or "Nick Name")

    lua_thread.create(function()
        pcall(function() sampSendChat("/todo Открепил рацию с пояса и поднёс к рту*" .. nick) end)
        wait(1000)

        if isFound then
            pcall(function() sampSendChat("/do На другом конце рации раздался отборный мат в адрес гражданина.") end)
            wait(1000)
            pcall(function() sampSendChat("/todo С улыбкой на лице*Сэр, мне сказали что вы не подходите.") end)
        else
            pcall(function() sampSendChat("/do На другом конце рации раздался отборный мат в адрес копа.") end)
            wait(1000)
            pcall(function() sampSendChat("/todo С наигранной улыбкой*Вас нет в ЧС МВД.") end)
        end
    end)
end

local function finalizeAndPrintResults()
    for k,v in pairs(history_capture.results) do
        writeLog("  key='"..tostring(k).."', level="..tostring(v))
    end
    if history_capture._direct_found then
        for k,v in pairs(history_capture._direct_found) do
            writeLog("  display='"..tostring(k).."', level="..tostring(v.level)..", normalized='"..tostring(v.normalized).."'")
        end
    end

    local merged = {}
    for name, lvl in pairs(history_capture.results) do merged[name] = lvl end
    if history_capture._direct_found then
        for display, info in pairs(history_capture._direct_found) do
            if not merged[display] then merged[display] = info.level end
        end
    end

    local found = {}
    for name, lvl in pairs(merged) do
        table.insert(found, name)
    end

    local displayNick = history_capture.display_target or formatNickForChat(history_capture.target or "")

    if #found == 0 then
        sampAddChatMessage("{00FF00}[MVD] Ники игрока не обнаружены в ЧС МВД", -1)
        if not history_capture._roleplay_sent then
            sendRoleplaySequenceAsync(displayNick, false)
            history_capture._roleplay_sent = true
        end
    else
        sampAddChatMessage("{FF3333}[MVD] Найдено в ЧС МВД: "..table.concat(found, ", "), -1)
        if not history_capture._roleplay_sent then
            sendRoleplaySequenceAsync(displayNick, true)
            history_capture._roleplay_sent = true
        end
    end

    history_capture._direct_found = nil
    resetHistoryCapture()
end

local function startHistoryCaptureFor(target, displayTarget)
    history_capture.active = true
    history_capture.expecting_dialog = false
    history_capture.target = normalize_for_index(target)
    history_capture.display_target = displayTarget or formatNickForChat(target)
    history_capture.pages_visited = 0
    history_capture.results = {}
    history_capture._direct_found = history_capture._direct_found or {}
    history_capture._roleplay_sent = history_capture._roleplay_sent or false
    history_capture._check_timestamp = os.clock()
    sampSendChat("/history "..target)
end

-- handle /mvdcheck
local function handleMvdCheckCommand(param)
    if not param or param == "" then
        sampAddChatMessage("{0099FF}[MVD] Использование: /mvdcheck Nick_Name", -1)
        return
    end
    if not loaded then
        sampAddChatMessage("{FF9900}[MVD] ЧС МВД не загружен. Используйте /mvdreload", -1)
        return
    end
    resetHistoryCapture()
    local target = param:match("%S+")
    if not target then return end

    writeLog("Requested check for: "..tostring(target))

    local lvl, matchedVariant = checkName(target)
    if lvl then
        history_capture.results[target] = lvl
        history_capture._direct_found = history_capture._direct_found or {}
        history_capture._direct_found[target] = { level = lvl, normalized = normalize_for_index(target) }
        if DEBUG_LOG_DUMP_MATCHES then
            local sub = normalize_for_index(target)
            dumpBlacklistMatches(sub, 50)
        end
        startHistoryCaptureFor(target, formatNickForChat(target))
        return
    end

    startHistoryCaptureFor(target, formatNickForChat(target))
end

-- load CSV & self-update checks
local function loadBlacklistAndSelfUpdate()
    blacklist = {}
    loaded = false

    dl_csv.attempt = 0
    dl_csv.inProgress = false
    dl_csv.nextAttemptTime = 0
    dl_csv.last_status = nil
    dl_csv.tempPath = nil

    dl_self.attempt = 0
    dl_self.inProgress = false
    dl_self.nextAttemptTime = 0
    dl_self.last_status = nil
    dl_self.tempPath = nil

    local okLocal = tryLoadFromLocalCSV()
    if okLocal then
        writeLog("Local CSV loaded for immediate use.")
    else
        writeLog("No local CSV present or failed to parse, will download.")
    end

    -- start downloads (CSV always, self only if URL present)
    startDownloadGeneric(url_csv, path_csv, dl_csv, "CSV")
    if url_self and url_self ~= "" then
        startDownloadGeneric(url_self, path_self, dl_self, "SELF")
    else
        writeLog("Self-update URL not configured; skipping .lua auto-check.")
    end
end

function sampev.onShowDialog(dialogId, dialogStyle, title, button1, button2, text)
    if not history_capture.active then return end
    if not title or not title:find("Прошлые имена") then return end

    if os.clock() - history_capture._check_timestamp > 30 then
        sampAddChatMessage("{FF9900}[MVD] История не получена (таймаут). Попробуйте ещё раз.", -1)
        writeLog("History capture timed out for target: "..tostring(history_capture.target))
        resetHistoryCapture()
        return
    end

    for line in text:gmatch("[^\r\n]+") do
        local t = trim(line)
        if t ~= "" then
            local name = extractNameFromHistoryLine_server(t)
            if name and name ~= "" then
                local lvl, matchedVariant = checkName(name)
                if lvl then
                    history_capture.results[name] = lvl
                end
            end
        end
    end

    history_capture.pages_visited = (history_capture.pages_visited or 0) + 1

    local pageBtn = nil
    local closeBtn = nil

    if button1 and isPageButtonText_server(button1) then pageBtn = 1 end
    if button2 and isPageButtonText_server(button2) then
        if pageBtn == nil then pageBtn = 2 else
            if button2:find(">>") or button2:find(">") or button2:lower():find("стр") then pageBtn = 2 end
        end
    end

    if button1 and isCloseButtonText_server(button1) then closeBtn = 1 end
    if button2 and isCloseButtonText_server(button2) then closeBtn = 2 end

    if history_capture.pages_visited < history_capture.max_pages and pageBtn then
        pcall(function() sampSendDialogResponse(dialogId, pageBtn, 0, "") end)
        history_capture.expecting_dialog = true
        return
    end

    if closeBtn then
        pcall(function() sampSendDialogResponse(dialogId, closeBtn, 0, "") end)
    else
        pcall(function() sampSendDialogResponse(dialogId, 1, 0, "") end)
    end

    finalizeAndPrintResults()
end

function sampev.onDialogResponse(dialogId, button, listbox, input)
end

function sampev.onServerMessage(color, text)
    if history_capture.active and text then
        if text:find("Не удалось найти игрока") or text:find("Игрок не найден") then
            sampAddChatMessage("{00FF00}[MVD] Игрок не найден", -1)
            resetHistoryCapture()
        end
    end
end

function main()
    repeat wait(100) until isSampAvailable()

    -- minimal startup message (ready will be announced once when appropriate)
    announceReadyOnce()

    loadBlacklistAndSelfUpdate()
    sampRegisterChatCommand("mvdcheck", function(param) handleMvdCheckCommand(param) end)

    -- mvdreload: запускает перезагрузку и сообщает о состоянии
    sampRegisterChatCommand("mvdreload", function()
        -- reset ready flag so reload can announce ready again once if desired
        ready_announced = false
        loadBlacklistAndSelfUpdate()
        if loaded then
            sampAddChatMessage("{00FF00}[MVD] Перезагрузка завершена — локальный CSV загружен, всё в порядке. ("..os.date("%Y-%m-%d %H:%M:%S")..")", -1)
            announceReadyOnce()
        else
            sampAddChatMessage("{0099FF}[MVD] Перезагрузка запущена — загрузка CSV выполняется асинхронно. Статус придёт в чат при завершении.", -1)
        end
    end)

    local last_periodic_check = 0

    while true do
        -----------------------------------------------------------
        -- CSV download finished?
        -----------------------------------------------------------
        if dl_csv.last_status ~= nil then
            local status = dl_csv.last_status
            dl_csv.last_status = nil
            local tmp = dl_csv.tempPath
            if status == 58 and tmp then
                writeLog("CSV download finished to temp: "..tostring(tmp))
                if not fileExists(path_csv) then
                    local ok, err = replaceFile(tmp, path_csv)
                    if ok then
                        local parsed = parseCSVToTable(path_csv)
                        if parsed and parsed.unique >= 5 then
                            applyParsedTable(parsed)
                            sampAddChatMessage("{0099FF}[MVD] CSV загружен и применён (новый файл).", -1)
                            announceReadyOnce()
                            writeLog("CSV new file applied. unique="..tostring(parsed.unique))
                        else
                            writeLog("Downloaded CSV parsed but failed validation; unique="..tostring(parsed and parsed.unique or "nil"))
                            sampAddChatMessage("{FF9900}[MVD] Загруженный CSV не прошёл валидацию; сохранён, но не применён.", -1)
                        end
                    else
                        writeLog("Failed to move downloaded CSV into place: "..tostring(err))
                        sampAddChatMessage("{FF9900}[MVD] Не удалось обновить CSV на диск. См. лог.", -1)
                        if fileExists(tmp) then os.remove(tmp) end
                    end
                else
                    if filesEqual(tmp, path_csv) then
                        writeLog("Downloaded CSV identical to local copy — skipping apply.")
                        os.remove(tmp)
                        announceReadyOnce()
                    else
                        local ok, err = replaceFile(tmp, path_csv)
                        if ok then
                            local parsed = parseCSVToTable(path_csv)
                            if parsed and parsed.unique >= 5 then
                                applyParsedTable(parsed)
                                sampAddChatMessage("{0099FF}[MVD] CSV обновлён (найдена новая версия на GitHub).", -1)
                                announceReadyOnce()
                                writeLog("CSV updated from remote. unique="..tostring(parsed.unique))
                            else
                                writeLog("New CSV replaced local file but failed validation; parsed.unique="..tostring(parsed and parsed.unique or "nil"))
                                sampAddChatMessage("{FF9900}[MVD] Новая CSV версия загружена, но не прошла валидацию.", -1)
                            end
                        else
                            writeLog("Failed to replace CSV with downloaded temp: "..tostring(err))
                            sampAddChatMessage("{FF9900}[MVD] Не удалось применить загруженный CSV. См. лог.", -1)
                            if fileExists(tmp) then os.remove(tmp) end
                        end
                    end
                end

                dl_csv.attempt = 0
                dl_csv.nextAttemptTime = os.time() + UPDATE_INTERVAL_SECONDS
                writeLog("Next CSV full update scheduled at "..tostring(os.date("%Y-%m-%d %H:%M:%S", dl_csv.nextAttemptTime)))
            else
                if status ~= 58 then
                    writeLog("CSV download failed with status: "..tostring(status))
                    if dl_csv.attempt < dl_csv.maxAttempts then
                        scheduleNextAttemptFor(dl_csv, "CSV")
                    else
                        writeLog("CSV download attempts exhausted.")
                        if not tryLoadFromLocalCSV() then
                            sampAddChatMessage("{FF9900}[MVD] Не удалось загрузить ЧС МВД. Подробности в mvd_error.log", -1)
                            writeLog("All CSV download attempts failed and no local CSV found.")
                        else
                            announceReadyOnce()
                        end
                        dl_csv.nextAttemptTime = os.time() + UPDATE_INTERVAL_SECONDS
                        writeLog("Next CSV full update scheduled at "..tostring(os.date("%Y-%m-%d %H:%M:%S", dl_csv.nextAttemptTime)))
                    end
                end
                if dl_csv.tempPath and fileExists(dl_csv.tempPath) then os.remove(dl_csv.tempPath) end
            end
        end

        -----------------------------------------------------------
        -- SELF (.lua) download finished?
        -----------------------------------------------------------
        if dl_self.last_status ~= nil then
            local status = dl_self.last_status
            dl_self.last_status = nil
            local tmp = dl_self.tempPath
            if status == 58 and tmp then
                writeLog("SELF download finished to temp: "..tostring(tmp))
                if not fileExists(path_self) then
                    local ok, err = replaceFile(tmp, path_self)
                    if ok then
                        sampAddChatMessage("{0099FF}[MVD] Скрипт (.lua) загружен на диск (новый файл).", -1)
                        writeLog("SELF applied: new .lua saved.")
                        announceReadyOnce()
                    else
                        writeLog("Failed to move downloaded SELF into place: "..tostring(err))
                        sampAddChatMessage("{FF9900}[MVD] Не удалось сохранить новый .lua. См. лог.", -1)
                        if fileExists(tmp) then os.remove(tmp) end
                    end
                else
                    if filesEqual(tmp, path_self) then
                        writeLog("Downloaded .lua identical to local copy — skipping apply.")
                        os.remove(tmp)
                    else
                        local ok, err = replaceFile(tmp, path_self)
                        if ok then
                            sampAddChatMessage("{0099FF}[MVD] Обнаружена новая версия скрипта на GitHub. .lua сохранён на диск.", -1)
                            writeLog("SELF updated from remote.")
                            announceReadyOnce()
                        else
                            writeLog("Failed to replace SELF with downloaded temp: "..tostring(err))
                            sampAddChatMessage("{FF9900}[MVD] Не удалось применить загруженный .lua. См. лог.", -1)
                            if fileExists(tmp) then os.remove(tmp) end
                        end
                    end
                end

                dl_self.attempt = 0
                dl_self.nextAttemptTime = os.time() + UPDATE_INTERVAL_SECONDS
                writeLog("Next SELF full update scheduled at "..tostring(os.date("%Y-%m-%d %H:%M:%S", dl_self.nextAttemptTime)))
            else
                if status ~= 58 then
                    writeLog("SELF download failed with status: "..tostring(status))
                    if dl_self.attempt < dl_self.maxAttempts then
                        scheduleNextAttemptFor(dl_self, "SELF")
                    else
                        writeLog("Self-update download attempts exhausted.")
                        dl_self.nextAttemptTime = os.time() + UPDATE_INTERVAL_SECONDS
                        writeLog("Next SELF full update scheduled at "..tostring(os.date("%Y-%m-%d %H:%M:%S", dl_self.nextAttemptTime)))
                    end
                end
                if dl_self.tempPath and fileExists(dl_self.tempPath) then os.remove(dl_self.tempPath) end
            end
        end

        -- start downloads if needed (CSV)
        if not dl_csv.inProgress and (dl_csv.nextAttemptTime == 0 or os.time() >= dl_csv.nextAttemptTime) and dl_csv.attempt < dl_csv.maxAttempts then
            startDownloadGeneric(url_csv, path_csv, dl_csv, "CSV")
        end

        -- start self download if configured
        if url_self and url_self ~= "" then
            if not dl_self.inProgress and (dl_self.nextAttemptTime == 0 or os.time() >= dl_self.nextAttemptTime) and dl_self.attempt < dl_self.maxAttempts then
                startDownloadGeneric(url_self, path_self, dl_self, "SELF")
            end
        end

        wait(1000)
    end
end
