function top(flow, top_size)
  info("Top size:"..tostring(top_size))


  local function transformer(rec)
    info("rec:"..tostring(rec))
    local touple = map()
    touple["eventid"] = rec["eventid"]
    touple["time"] = rec["time"]
    info("touple:"..tostring(touple))
    return touple
  end
 
  local function movedown(theList, size, at, element)
    info("List:"..tostring(theList)..":"..tostring(size)..":"..tostring(start))
    if at > size then
      info("You are an idiot")
      return
    end 
    for i = size-1, i > at, -i do
      theList[i+1] = theList[i]
    end
    theList[at] = element
  end
  
  local function accumulate(aggregate, nextitem)
    local size = list.size(aggregate)
    if size == 0 then
      aggregate[1] = nextitem
    else  
      for i = 1, i <= size, 1 do
        if nextitem.time > aggregate[i].time then
          movedown(aggregate, top_size, i, nextitem)
          break
        end
      end
    end
    return aggregate
  end

  local function reducer( this, that )
    return list.merge(this, that)
  end
  
  flow:map(transformer)--:aggregate(list{}, accumulate):reduce(reducer)

end