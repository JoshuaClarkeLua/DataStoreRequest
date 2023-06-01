local DataStoreRequest = {}

export type RequestType = "GetAsync" | "SetAsync" | "IncrementAsync" | "UpdateAsync" | "RemoveAsync" | "GetSortedAsync" | "GetVersionAsync" | "ListKeysAsync" | "RemoveVersionAsync"

-- RBX SERVICES
local ReplicatedStorage = game:GetService("ReplicatedStorage")
local DataStoreService = game:GetService("DataStoreService")
local Players = game:GetService("Players")
-- PACKAGES
local Signal = require(script.Parent.Parent.Signal)
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

local function startRequestQueueLoop(queue)
	repeat
		local request = table.remove(queue, 1)
		while getRequestBudget(request) <= 0 do
			task.wait(3)
		end
		local func = request.datastore[request.type]
		local vals = table.pack(pcall(func, request.datastore, table.unpack(request.args)))
		request.finished = true
		task.defer(onRequestFinished.FireDeferred, onRequestFinished, request, vals)
		task.wait(getRequestWait(request))
	until #queue == 0
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

function DataStoreRequest.queueAsync(datastore: GlobalDataStore, requestType: RequestType, maxRetries: number, ...: any): (boolean, ...any)
	assert(REQUEST_TYPE_ENUM[requestType], "Invalid request type")
	assert(type(maxRetries) == "number", "Invalid retry amount")
	assert(typeof(datastore) == "Instance" and datastore:IsA("GlobalDataStore"), "Invalid datastore")

	local thisRequest = {
		datastore = datastore,
		type = requestType,
		args = table.pack(...),
		finished = false,
		result = nil,
	}
	local retry = 0
	local success, vals
	repeat
		if success == false then
			thisRequest.finished = false
			task.wait(REQUEST_FAIL_WAIT)
		end
		onRequestAdded:Fire(thisRequest)
		if not thisRequest.result then
			repeat
				_, vals = onRequestFinished:Wait()
				success = vals[1]
			until thisRequest.finished
		end
		retry += 1
	until success or retry > maxRetries
	return success, table.unpack(vals, 2)
end

function DataStoreRequest.queue(datastore: GlobalDataStore, requestType: RequestType, maxRetries: number, callback: (...any) -> (), errorHandler: (err: string) -> (), ...: any): ()
	assert(typeof(datastore) == "Instance" and datastore:IsA("GlobalDataStore"), "Invalid datastore")
	assert(REQUEST_TYPE_ENUM[requestType], "Invalid request type")
	assert(type(maxRetries) == "number", "Invalid retry amount")
	assert(type(callback) == "function", "Invalid callback function")
	assert(type(errorHandler) == "function", "Invalid errorHandler function")

	local thisRequest = {
		datastore = datastore,
		type = requestType,
		args = table.pack(...),
		finished = false,
		result = nil,
	}
	local retry = 0
	local conn
	conn = onRequestFinished:Connect(function(_, vals)
		if not thisRequest.finished then return end
		retry += 1
		local success = vals[1]
		if success then
			task.spawn(callback, table.unpack(vals, 2))
		else
			if retry <= maxRetries then
				thisRequest.finished = false
				task.wait(REQUEST_FAIL_WAIT)
				onRequestAdded:Fire(thisRequest)
				return
			end
			task.spawn(errorHandler, table.unpack(vals, 2))
		end
		conn:Disconnect()
	end)
	onRequestAdded:Fire(thisRequest)
end


init()
return DataStoreRequest
