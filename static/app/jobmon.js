(function() {
    var app = angular.module('babynames', ['ngCookies', 'chart.js']);
    app.Root = '/';
    app.config(['$interpolateProvider',
        function($interpolateProvider) {
            $interpolateProvider.startSymbol('{[');
            $interpolateProvider.endSymbol(']}');
        }
    ]);	
    function run($rootScope, $location, $http, $scope) {}

    app.controller('JobMonControl', function($rootScope, $scope, $http, $timeout, $cookies) {
        $rootScope.updateInterval = 3000;
        $scope.options = {
            animation: false,
            showScale: true,
            showTooltips: true,
            pointDot: false,
            datasetStrokeWidth: 2.0,
            showLegend: true,
            interactivityEnabled: false,
            scaleShowHorizontalLines: true,
            scaleShowVerticalLines: false,
            datasetFill: true,
            scaleOverride: true,
            scaleStartValue: 0,
            scaleStepWidth: .5,
            scaleSteps: 40,
            responsive: true,
            //maintainAspectRatio: true,
            
        };

        //add new array to add another data set to the chart
        $scope.chartData = [
            [],[],[],[],[],[]
        ];
        
        $scope.chartLabels = [];
        $scope.chartSeries = [
            'Running',
            "Completed / 1000",
            'Fail %',
            'Enqueued / 1000',
            'Requeued / 10',
            'Fail Count 15 min / 10'
        ];

        $scope.failed_count = 0;
        $scope.running = 0;
        $scope.requeued = 0;
        $scope.completed = 0;
        $scope.queue_count = 0;
        $scope.last_15m_fails = 0;

        $scope.addMessage = "";

        $scope.countDown = 50;
        $scope.checkData = function() {
            
            $http.get(app.Root + 'data')
                .success(function(data, status, headers, config) {
                    $scope.running = data.running;
                    $scope.queue_count = data.queue_count;
                    $scope.failed_count = ((data.failed_count / data.completed) * 100).toFixed(2);
                    $scope.requeued = data.requeued;
                    $scope.completed = data.completed;
                    $scope.last_15m_fails = data.last_15m_fails;

                    if ($scope.queue_count >= 1 || $scope.countDown > 0) {
                        $scope.chartData[0].push(data.running);
                        $scope.chartData[1].push(data.completed / 1000);
                        $scope.chartData[2].push(((data.failed_count / data.completed) * 100).toFixed(2));
                        $scope.chartData[3].push(data.queue_count / 1000);
                        $scope.chartData[4].push(data.requeued / 10);
                        $scope.chartData[5].push(data.last_15m_fails / 10);
                        
                        $scope.chartLabels.push(moment().format("HH:mm:ss"));

                        if ($scope.queue_count == 0) {
                            $scope.countDown = $scope.countDown - 1;
                        }

                        if ($scope.chartLabels.length > 70) {
                            $scope.chartData[5].shift();
                            $scope.chartData[4].shift();
                            $scope.chartData[3].shift();
                            $scope.chartData[2].shift();
                            $scope.chartData[1].shift();
                            $scope.chartData[0].shift();
                            $scope.chartLabels.shift();
                        }
                    }
                }).error(function(data, status, headers, config) {});
            if ($scope.countDown == 0 && data.queue_count !== 0){
                $scope.countDown = 50;
            }
        };
        
        $scope.queue_jobs = function(jobs) {
            $http.get(app.Root + 'queue?count=' + jobs)
                .success(function(data, status, headers, config) {
                    if (data.message !== undefined) {
                        $scope.addMessage = data.message;                        
                    }
                    else {
                        $scope.addMessage = undefined;
                    }
                })
                .error(function(data, status, headers, config) {});
        };

        $scope.flushRedis = function() {
            $http.get(app.Root + 'flushall')
                .success(function(data, status, headers, config) {})
                .error(function(data, status, headers, config) {});
        };
        
        $scope.checkData();
        $scope.intervalFunction = function() {
            $timeout(function() {
                $scope.checkData();
                $scope.intervalFunction();
            }, $rootScope.updateInterval)
        };
        $scope.intervalFunction();
    });
})();
