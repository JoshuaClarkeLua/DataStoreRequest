-- Scripted by JCL
--!strict
local DataStoreRequest = {}

export type RequestType = "GetAsync" | "SetAsync" | "IncrementAsync" | "UpdateAsync" | "RemoveAsync" | "GetSortedAsync" | "GetVersionAsync" | "ListKeysAsync" | "RemoveVersionAsync"

type Request = {
	maxRetries: number,
	try: number,
	store: GlobalDataStore,
	type: RequestType,
	args: {any},
	callback: ((...any) -> ())?,
	errorHandler: ((...any) -> ())?,
	result: {any}?,
}

-- RBX SERVICES
local DataStoreService = game:GetService("DataStoreService")
local Players = game:GetService("Players")
-- PACKAGES
local Signal = require(script.Parent.Signal)
--
local REQUEST_TYPE_ENUM = {
	["GetAsync"] = Enum.DataStoreRequestType.GetAsync,
	["SetAsync"] = Enum.DataStoreRequestType.UpdateAsync,
	["IncrementAsync"] = Enum.DataStoreRequestType.SetIncrementAsync,
	["UpdateAsync"] = Enum.DataStoreRequestType.UpdateAsync,
	["RemoveAsync"] = Enum.DataStoreRequestType.UpdateAsync,
	--
	["GetSortedAsync"] = Enum.DataStoreRequestType.GetSortedAsync,
	["GetVersionAsync"] = Enum.DataStoreRequestType.GetSortedAsync,
	["ListKeysAsync"] = Enum.DataStoreRequestType.GetSortedAsync,
	["RemoveVersionAsync"] = Enum.DataStoreRequestType.GetSortedAsync,
}
local REQUEST_FAIL_WAIT = 20
local REQUESTS_PER_USER = {
	["Default"] = 10,
	[Enum.DataStoreRequestType.GetSortedAsync] = 2,
}
local requestQueues = {
	["Default"] = {},
	[Enum.DataStoreRequestType.GetSortedAsync] = {},
}
local onRequestAdded = Signal.new()
local onRequestFinished = Signal.new()

local function getRequestType(request)
	return REQUEST_TYPE_ENUM[request.type]
end

local function getRequestWait(request)
	local requestPerUser = REQUESTS_PER_USER[getRequestType(request)] or REQUESTS_PER_USER["Default"]
	return 60 / (60 + (#Players:GetPlayers() * requestPerUser))
end

local function getRequestBudget(request): number
	local budget = DataStoreService:GetRequestBudgetForRequestType(getRequestType(request))
	return budget
end

local function getRequestQueue(request)
	local reqType = getRequestType(request)
	return requestQueues[reqType] or requestQueues["Default"]
end

local function startRequestQueueLoop(queue: {Request}): ()
	local request = table.remove(queue, 1)
	while request do
		-- Wait for requests to be available
		while getRequestBudget(request) <= 0 do
			task.wait(3)
		end
		--
		request.try += 1
		local func = request.store[request.type]
		local vals = table.pack(pcall(func, request.store, table.unpack(request.args)))

		local success = vals[1]
		if success and request.callback ~= nil then
			task.spawn(request.callback, table.unpack(vals, 2))
		elseif not success and request.try <= request.maxRetries then
			task.spawn(function()
				task.wait(REQUEST_FAIL_WAIT)
				onRequestAdded:Fire(request)
			end)
		elseif request.errorHandler ~= nil then
			task.spawn(request.errorHandler, table.unpack(vals, 2))
		else
			request.result = vals
			task.defer(onRequestFinished.FireDeferred, onRequestFinished, request, vals)
		end

		task.wait(getRequestWait(request))
		request = table.remove(queue, 1)
	end
end

local function listenForQueueRequests(queue)
	local started = false
	onRequestAdded:Connect(function(request)
		local requestQueue = getRequestQueue(request)
		if not requestQueue or requestQueue ~= queue then return end
		table.insert(queue, request)
		if started then return end
		started = true
		startRequestQueueLoop(queue)
		started = false
	end)
end

local function init()
	listenForQueueRequests(requestQueues["Default"])
	listenForQueueRequests(requestQueues[Enum.DataStoreRequestType.GetSortedAsync])
end

function DataStoreRequest.queueAsync(store: GlobalDataStore, requestType: RequestType, maxRetries: number, ...: any): (boolean, ...any)
	assert(REQUEST_TYPE_ENUM[requestType], "Invalid request type")
	assert(type(maxRetries) == "number", "Invalid retry amount")
	assert(typeof(store) == "Instance" and store:IsA("GlobalDataStore"), "Invalid datastore")

	local request: Request = {
		maxRetries = maxRetries,
		try = 0,
		store = store,
		type = requestType,
		args = table.pack(...),
	}

	onRequestAdded:Fire(request)

	while not request.result do
		onRequestFinished:Wait()
	end

	if not request.result then return false end
	return table.unpack(request.result)
end

function DataStoreRequest.queue(store: GlobalDataStore, requestType: RequestType, maxRetries: number, callback: (...any) -> ()?, errorHandler: (err: string) -> ()?, ...: any): ()
	assert(typeof(store) == "Instance" and store:IsA("GlobalDataStore"), "Invalid datastore")
	assert(REQUEST_TYPE_ENUM[requestType], "Invalid request type")
	assert(type(maxRetries) == "number", "Invalid retry amount")
	assert(type(callback) == "function", "Invalid callback function")
	assert(type(errorHandler) == "function", "Invalid errorHandler function")

	onRequestAdded:Fire({
		maxRetries = maxRetries,
		try = 0,
		store = store,
		type = requestType,
		args = table.pack(...),
		callback = callback,
		errorHandler = errorHandler,
	})
end


init()
return DataStoreRequest
