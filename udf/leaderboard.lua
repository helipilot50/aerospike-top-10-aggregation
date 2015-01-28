local DEBUG = true
local GD

function top(flow, top_size)
  --info("Top size:"..tostring(top_size))

  local function transformer(rec)
    --info("rec:"..tostring(rec))
    local touple = map()
    touple["eventid"] = rec["eventid"]
    touple["time"] = rec["time"]
    --info("touple:"..tostring(touple))
    return touple
  end

  local function movedown(theList, size, at, element)
    --info("List:"..tostring(theList)..":"..tostring(size)..":"..tostring(start))
    if at > size then
      info("You are an idiot")
      return
    end 
    index = size-1
    while (index > at) do
      theList[index+1] = theList[index]
      index = index -1
    end
    
    theList[at] = element
  end
  
  local function accumulate(aggregate, nextitem)
    local aggregate_size = list.size(aggregate)
      info("Accumulator - size:"..tostring(aggregate_size).." Aggregate:"..tostring(aggregate))
      --info("Item:"..tostring(nextitem))
      index = 1
      for value in  list.iterator(aggregate) do
        --info(tostring(nextitem.time).." > "..tostring(value.time))
        if nextitem.time > value.time then
          movedown(aggregate, top_size, index, nextitem)
          break
        end
        index = index + 1
      end
    return aggregate
  end

  local function reducer( this, that )
    local merged_list = list()
    local this_index = 1
    local that_index = 1
    while this_index <= 10 do
      while that_index <= 10 do
        if this[this_index].time >= that[that_index].time then
          list.append(merged_list, this[this_index])
          this_index = this_index + 1
        else
          list.append(merged_list, that[that_index])
          that_index = that_index +1
        end
        if list.size(merged_list) == 10 then
          break
        end
      end
      if list.size(merged_list) == 10 then
        break
      end
    end
    --info("This:"..tostring(this).." that:"..tostring(that))
    return merged_list
  end
  
  return flow:map(transformer):aggregate(
          list{
              map{eventid="ten",time=0},
              map{eventid="nine",time=0},
              map{eventid="eight",time=0},
              map{eventid="seven",time=0},
              map{eventid="six",time=0},
              map{eventid="five",time=0},
              map{eventid="four",time=0},
              map{eventid="three",time=0},
              map{eventid="two",time=0},
              map{eventid="one",time=0}
          }, accumulate):reduce(reducer)

end