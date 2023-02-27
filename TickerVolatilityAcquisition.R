#Here we load the necessary packages, and install them as necessary.
#An internet connection is required for this process (and for the success of this program.)

qm = require('quantmod')
other = require('fs')
if(qm == FALSE){
  install.packages('quantmod')
}
if(other == FALSE){
  install.packages('fs')
}


#With one function, we acquire all the necessary information pertaining to a list of Tickers.
#The output will consist of csv files each of which relate parkinsons volatility to a given ticker.
#In the absence of a specified directory, the output will default to a directory generated in the user's
#home directory.
#The main 'Fund Operation' directory will also be created unless it already exists.

#Pos maybe used to begin at a certain position in a file for debugging purposes.

acquireTickers = function(inp,outdir = 0,pos = 1){
  wd = path_join(c(path_expand('~'),'FundOperation'))
  if(dir.exists(wd) == FALSE){
    dir.create(wd)
  }
  if(outdir == 0){
    goalDir = path_join(c(wd,'VolatilityData'))
    if(dir.exists(goalDir) == FALSE){
      dir.create(goalDir)
  }
  }
  else{
    goalDir = outdir
  }
  df = read.csv(inp)
  myTickers = df$Ticker
  
  for(i in pos:length(myTickers)){
    cond = FALSE
    attempt = try(expr = {currTic = getSymbols(myTickers[i],env = NULL,from = '2019-01-01',to = Sys.Date() + 1)[,1:4]
    Sys.sleep(runif(1,min = 3,max = 5))},silent = T)
    if(class(attempt) == "try-error"){
      Sys.sleep(runif(1,min = 10,max = 13))
      next
    }
    else{
      toplot = volatility(currTic,calc='parkinson',n=63)
      toplot = na.omit(toplot)
      colnames(toplot) <- 'Parkinson_Volatility_63DayInterval'
      write.csv(toplot,file = path_join(c(goalDir,paste(myTickers[i],'.csv',sep = ''))))
      
    }
  }
}



