from Redis import Redis
from statistics import pstdev, mean, median, median_grouped
from dateutil import parser

class JobsRuntimeKPIRoller:
    rares = []
    long_running = []
    
    
    def __init__(self):
        self.R = Redis().Connection
        


    def __run_secs(self, id, bucket):
        start = "STARTTIME:" + str(bucket) + ":" + str(id)
        end = "ENDTIME:" + str(bucket) + ":" + str(id)
        rs = self.R.get(start)
        re = self.R.get(end)
        if rs is not None and re is not None:
            rs = rs.decode('utf8')
            re = re.decode('utf8')
        s = parser.parse(rs)
        e = parser.parse(re)
        return (e - s).seconds
        

    def __find_outliers(self):
        for id in self.ids:
            time = self.id_time_dict[id]
            if time > (self.mean + (self.std * 2)):
                self.rares.append({
                    "id" : id,
                    "seconds": time})

    
    def export_runtime_kpi(self, bucket):
        ids = []
        times = []

        for v in self.R.smembers("COMPLETE:" + bucket):
            times.append(self.__run_secs(v.decode('utf8'), bucket)) 
            ids.append(int(v.decode('utf8')))

        self.id_time_dict = dict(zip(ids, times))
        self.ids = ids
        self.times = times
        self.total = len(times)
        self.max_run = max(times)
        self.mean = mean(times)
        self.median = median(times)
        self.median_grouped = median_grouped(times)
        self.std = pstdev(times)
        self.__find_outliers()


    def print_results(self):
        print('Max: ' + str(self.max_run))
        print('Total: ' + str(self.total))
        print('Mean: ' + str(self.mean))
        print('Median: ' + str(self.median))
        print('Median grouped: ' + str(self.median_grouped))
        print('Standard Deviation: ' + str(self.std))
        #print(self.id_time_dict)
        print('Rares: ' + str(len(self.rares)))
        print('Rare %: ' +  str((len(self.rares) / self.total) * 100))

        
    def results(self):
        return {
            "total": self.total,
            "max": self.max_run,
            "mean": self.mean,
            "median": self.median,
            "media_grouped": self.median_grouped,
            "standardDeviation": self.std,
            "rareCount": len(self.rares),
            "rarePercent": (len(self.rares) / self.total) * 100,
            "rares": self.rares
        }


if __name__ == '__main__':
    bucket = "DEMO"
    j = JobsRuntimeKPIRoller()
    j.export_runtime_kpi(bucket)
    j.print_results()
