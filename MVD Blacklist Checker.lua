-- MVD Blacklist Checker (self-update + always redownload CSV on join)
-- Автор: qqwekky (модификация под запрос пользователя)
-- Версия: v1.12-mod (самообновление при заходе, убран mvd_last_update.txt)

script_name("MVD Blacklist Checker")
script_author("qqwekky")
script_version("v1.12-mod")

require "lib.moonloader"
local sampev = require "lib.samp.events"

-- URLы
local url_csv = "https://raw.githubusercontent.com/qqwekky/mvd-data/main/mvd_blacklist.csv"
local url_self = "https://raw.githubusercontent.com/qqwekky/mvd-data/refs/heads/main/MVD%20Blacklist%20Checker.lua"

-- Пути (оставляем только два файла: csv и лог)
local path_csv = getWorkingDirectory() .. "\\mvd_blacklist.csv"
local logpath = getWorkingDirectory() .. "\\mvd_error.log"
-- путь до самого lua-файла в рабочей директории (имя файла должно совпадать с тем, что используется локально)
local path_self = getWorkingDirectory() .. "\\MVD Blacklist Checker.lua"

-- Интервалы и константы (оставляем обновление при старте, периодика опциональна)
local UPDATE_INTERVAL_DAYS = 3
local UPDATE_INTERVAL_SECONDS = UPDATE_INTERVAL_DAYS * 24 * 60 * 60

-- Состояние данных
local blacklist = {}
local loaded = false
local rp_enabled = true

-- Отдельные состояния скачивания: для CSV и для self-update
local dl_csv = { attempt = 0, maxAttempts = 3, nextAttemptTime = 0, inProgress = false, last_status = nil }
local dl_self = { attempt = 0, maxAttempts = 3, nextAttemptTime = 0, inProgress = false, last_status = nil }

-- Отладочные флаги (сохраняем как в оригинале)
local DEBUG_LOG_LOOKUPS = true
local DEBUG_LOG_PAGING = true
local DEBUG_LOG_INDEXING = true
local DEBUG_LOG_DUMP_MATCHES = true

-- Вспомогательные функции
local function trim(s) if not s then return "" end return s:match("^%s*(.-)%s*$") end

local function writeLog(msg)
    local f = io.open(logpath, "a")
    if f then
        f:write("["..os.date("%Y-%m-%d %H:%M:%S").."] "..tostring(msg).."\n")
        f:close()
    end
end

local function fileExists(p) local f = io.open(p, "rb") if f then f:close() return true end return false end

-- Нормализация и генерация вариантов
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

local function splitCSV(line)
    local res, cur, inQuotes = {}, "", false
    for i = 1, #line do
        local c = line:sub(i,i)
        if c == '"' then
            inQuotes = not inQuotes
        elseif c == ',' and not inQuotes then
            table.insert(res, cur)
            cur = ""
        else
            cur = cur .. c
        end
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

-- Парсинг CSV по пути, общий для локального и загруженного файла
local function parseCSVAtPath(p)
    if not fileExists(p) then return false end
    local f = io.open(p, "rb")
    if not f then return false end
    local content = f:read("*all")
    f:close()
    if not content or content == "" then return false end
    if content:sub(1,3) == "\239\187\191" then content = content:sub(4) end

    blacklist = {}
    local count = 0
    for line in content:gmatch("[^\r\n]+") do
        local cols = splitCSV(line)
        for i = 1, #cols do
            local col = cols[i]
            if col and trim(col) ~= "" then
                if looks_like_nick(col) then
                    if addNick(col, 1) then count = count + 1 end
                else
                    if DEBUG_LOG_INDEXING then writeLog("Skipped non-nick column: '"..tostring(col).."'") end
                end
            end
        end
    end
    loaded = true
    writeLog("Parsed blacklist from '"..tostring(p).."', indexed entries: "..tostring(count))
    return true
end

-- Попытка загрузить из локального CSV для мгновенной доступности
local function tryLoadFromLocalCSV()
    if not fileExists(path_csv) then return false end
    local ok = parseCSVAtPath(path_csv)
    if ok then
        writeLog("Loaded blacklist from local file: "..tostring(path_csv))
    end
    return ok
end

-- Универсальный стартер загрузки с callback'ом состояния
local function startDownloadGeneric(downloadUrl, destPath, stateTable, label)
    if stateTable.inProgress then return end
    stateTable.attempt = stateTable.attempt + 1
    stateTable.inProgress = true
    writeLog("Starting download attempt "..tostring(stateTable.attempt).." for "..tostring(label).." url="..tostring(downloadUrl))
    downloadUrlToFile(downloadUrl, destPath, function(id, status)
        stateTable.last_status = status
        stateTable.inProgress = false
        writeLog("download callback for "..tostring(label).." attempt "..tostring(stateTable.attempt).." status="..tostring(status))
    end)
end

local function scheduleNextAttemptFor(stateTable, label)
    local backoff = 2 ^ (stateTable.attempt - 1)
    stateTable.nextAttemptTime = os.time() + backoff
    writeLog("Scheduling next attempt for "..tostring(label).." in "..tostring(backoff).."s")
end

-- История/проверки (как в оригинале)
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

function sampev.onShowDialog(dialogId, dialogStyle, title, button1, button2, text)
    if not history_capture.active then return end
    if not title or not title:find("Прошлые имена") then return end
    
    if os.clock() - history_capture._check_timestamp > 30 then
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

-- Инициализация загрузки blacklist и self-update, убрана логика с last_update.txt, всегда обновляем при старте
local function loadBlacklistAndSelfUpdate()
    blacklist = {}
    loaded = false

    dl_csv.attempt = 0
    dl_csv.inProgress = false
    dl_csv.nextAttemptTime = 0
    dl_csv.last_status = nil

    dl_self.attempt = 0
    dl_self.inProgress = false
    dl_self.nextAttemptTime = 0
    dl_self.last_status = nil

    -- Попытаться быстро загрузить локально CSV для мгновенных проверок
    local okLocal = tryLoadFromLocalCSV()
    if okLocal then
        writeLog("Local CSV loaded for immediate use.")
    else
        writeLog("No local CSV present or failed to parse, will download.")
    end

    -- Незамедлительно инициируем скачивание CSV и self lua-файла (каждый раз при заходе)
    dl_csv.attempt = 0
    dl_csv.nextAttemptTime = 0
    startDownloadGeneric(url_csv, path_csv, dl_csv, "CSV")

    dl_self.attempt = 0
    dl_self.nextAttemptTime = 0
    startDownloadGeneric(url_self, path_self, dl_self, "SELF")
end

function main()
    repeat wait(100) until isSampAvailable()

    sampAddChatMessage("{0099FF}[MVD] Разработчик: qqwekky (self-update enabled)", -1)
    sampAddChatMessage("{0099FF}[MVD] Команды: /mvdcheck - Чекер ЧС МВД; /mvdreload - Перезагрузка ЧС", -1)

    -- Запускаем загрузку локального CSV и попытки скачать CSV и сам скрипт
    loadBlacklistAndSelfUpdate()
    sampRegisterChatCommand("mvdcheck", function(param) handleMvdCheckCommand(param) end)
    sampRegisterChatCommand("mvdreload", function() loadBlacklistAndSelfUpdate() sampAddChatMessage("{0099FF}[MVD] Перезагрузка ЧС запущена", -1) end)

    local last_periodic_check = 0

    while true do
        -- Обработка результатов скачивания CSV
        if dl_csv.last_status ~= nil then
            local status = dl_csv.last_status
            dl_csv.last_status = nil
            if status == 58 then
                -- успешно скачано: распарсить и применить
                if parseCSVAtPath(path_csv) then
                    writeLog("CSV download parsed and applied.")
                    sampAddChatMessage("{0099FF}[MVD] CSV успешно обновлён.", -1)
                else
                    writeLog("Downloaded CSV exists but failed to parse.")
                end
                -- сброс попыток для будущих обновлений
                dl_csv.attempt = 0
                dl_csv.nextAttemptTime = 0
            else
                if dl_csv.attempt < dl_csv.maxAttempts then
                    scheduleNextAttemptFor(dl_csv, "CSV")
                else
                    writeLog("CSV download attempts exhausted.")
                    -- если есть локальный файл - продолжим использовать его, иначе предупредим
                    if not tryLoadFromLocalCSV() then
                        sampAddChatMessage("{FF9900}[MVD] Не удалось загрузить ЧС МВД. Подробности в mvd_error.log", -1)
                        writeLog("All CSV download attempts failed and no local CSV found.")
                    end
                end
            end
        end

        -- Обработка результатов скачивания SELF (lua-файла)
        if dl_self.last_status ~= nil then
            local status = dl_self.last_status
            dl_self.last_status = nil
            if status == 58 then
                writeLog("Self-update file downloaded to: "..tostring(path_self))
                sampAddChatMessage("{0099FF}[MVD] Скрипт обновлён на диск. Перезапустите скрипт или клиент, чтобы применить изменения.", -1)
                -- сброс попыток для будущих обновлений
                dl_self.attempt = 0
                dl_self.nextAttemptTime = 0
            else
                if dl_self.attempt < dl_self.maxAttempts then
                    scheduleNextAttemptFor(dl_self, "SELF")
                else
                    writeLog("Self-update download attempts exhausted.")
                end
            end
        end

        -- Попытки повторного запуска скачиваний при неуспехе и по расписанию
        -- CSV
        if not dl_csv.inProgress and dl_csv.attempt < dl_csv.maxAttempts then
            if dl_csv.nextAttemptTime == 0 or os.time() >= dl_csv.nextAttemptTime then
                startDownloadGeneric(url_csv, path_csv, dl_csv, "CSV")
            end
        end
        -- SELF
        if not dl_self.inProgress and dl_self.attempt < dl_self.maxAttempts then
            if dl_self.nextAttemptTime == 0 or os.time() >= dl_self.nextAttemptTime then
                startDownloadGeneric(url_self, path_self, dl_self, "SELF")
            end
        end

        -- Периодическая проверка на устаревание CSV (опционально, чтобы перекачивать позднее)
        if os.time() - last_periodic_check >= 60 then
            last_periodic_check = os.time()
            -- если CSV давно не обновлялся локально, инициируем повторную загрузку
            -- здесь простая эвристика: если локального файла нет, пробуем скачать, иначе оставляем периодические попытки на скачивание (по флагам dl_csv)
            if not fileExists(path_csv) and not dl_csv.inProgress then
                writeLog("Periodic check: no local CSV present -> starting CSV download.")
                dl_csv.attempt = 0
                dl_csv.nextAttemptTime = 0
                startDownloadGeneric(url_csv, path_csv, dl_csv, "CSV")
            end
        end

        wait(1000)
    end
end
