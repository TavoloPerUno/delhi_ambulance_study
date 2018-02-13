install.packages(sp)
install.packages(rgdal)
install.packages(parallel)
install.packages(data.table)
library(sp)
require(rgdal)
library(parallel)
# Read SHAPEFILE.shp from the current working directory (".")

setwd("/project2/kavibhalla/codebase/delhi_ambulance_study/data/Delhi/edges")
edges <- readOGR(".", "edges")

sample.line <- function(idx, gdf, sdist=100){
  lsub <- gdf[idx,]
  if (!require(sp)) stop("sp PACKAGE MISSING")
  if (!inherits(gdf, "SpatialLinesDataFrame")) stop("MUST BE SP SpatialLinesDataFrame OBJECT")
  
  lgth <- max(SpatialLinesLengths(lsub), 1)
  
  ns <- max(round( (lgth / sdist), digits=0), 1)
  lsamp <- spTransform(lsub,CRS("+proj=longlat"))
  if (ns > 1){
    lsamp <- spsample(lsub, n=ns, type="regular", offset=c(0))
    lsamp <- spTransform(lsamp,CRS("+proj=longlat"))
    lsamp <- SpatialPointsDataFrame(lsamp, data=data.frame(ID=rep(as.integer(idx),ns)))
    lsamp <- data.frame(LINE_ID=rep(as.integer(idx),ns), LNG=coordinates(lsamp)[,1],
                        LAT=coordinates(lsamp)[,2])
    
  }
  else{
    lsamp <- data.frame(LINE_ID=as.integer(idx), LNG=coordinates(lsamp)[[1]][[1]][1,1],
                        LAT=coordinates(lsamp)[[1]][[1]][1,2]) 
  }
  return(lsamp)
}

lpts <- mclapply(rownames(edges@data),sample.line, gdf=edges, sdist=10, mc.cores=64)

library(data.table)
pts <- rbindlist(lpts)
pts$ID <- c(1:nrow(pts))

write.csv(pts, 'delhi.csv', row.names=FALSE)
