<!DOCTYPE html>

<html lang="en">
    <head>
      <meta name="viewport" content="width=device-width, initial-scale=1">
      <link href="http://cdmo.baruch.sc.edu/resources/js/bootstrap/bootstrap-3.3.4/css/bootstrap.min.css" rel="stylesheet">
      <link href="http://cdmo.baruch.sc.edu/resources/js/bootstrap/bootstrap-3.3.4/css/bootstrap-theme.min.css" rel="stylesheet">

      <title>Sarasota Water Quality Prediction Results</title>
    </head>
    <body>
        <div class="container">
            <div class="row">
                <h1>Sarasota Water Quality Prediction Results</h1>
                <h2>Prediction for: ${prediction_date}</h2>
                <h3>Prediction executed: ${execution_date}</h3>
                </br>
            </div>
            <div class = "row">
              <div class="col-xs-12">
                <h2><a href='${report_url}'>Report File</a> Click here if the report below displays incorrectly.</h2>
              </div>
            </div>
            % for site_data in ensemble_tests:
                <div class="row">
                    <div class="col-xs-6">
                      <h2>Site: ${site_data['metadata'].name}</h2>
                    </div>
                </div>
                <div class="row">
                    <div class="col-xs-6">
                      <h3><span>Overall Prediction: ${str(site_data['models'].ensemblePrediction)}</span></h3>
                    </div>
                </div>
                % if site_data['entero_value'] is not None:
                    <div class="row">
                        <table class="table table-bordered">
                            <tr>
                                <th> Site Sampled Entero Value </th>
                            </tr>
                            <tr>
                                <td>
                                    ${"%.2f" % (site_data['entero_value'])}
                                </td>
                            </tr>
                        </table>
                    </div>
                % endif
                % if site_data['statistics'] is not None:
                    <div class="row">
                        <table class="table table-bordered">
                            <tr>
                                <th>Minimum Entero</th>
                                <th>Maximum Entero</th>
                                <th>Average Entero</th>
                                <th>Median Entero</th>
                                <th>StdDev Entero</th>
                            </tr>
                            <tr>
                              <td>${"%.2f" % (site_data['statistics'].minVal)}</td>
                              <td>${"%.2f" % (site_data['statistics'].maxVal)}</td>
                              <td>${"%.2f" % (site_data['statistics'].average)}</td>
                              <td>${"%.2f" % (site_data['statistics'].median)}</td>
                              <td>${"%.2f" % (site_data['statistics'].stdDev)}</td>
                            </tr>
                        </table>
                    </div>
                % endif
                <div class="row">
                    <table class="table table-bordered">
                        <tr>
                            <th>Model Name</th>
                            <th>Prediction Level</th>
                            <th>Prediction Value</th>
                            <th>Data Used</th>
                        </tr>
                        % for test_obj in site_data['models'].tests:

                            <tr>
                              <td>
                                ${test_obj.model_name}
                              </td>
                              <td>
                                ${test_obj.predictionLevel.__str__()}
                              </td>
                              <td>
                                % if test_obj.mlrResult is not None:
                                  ${"%.2f" % (test_obj.mlrResult)}
                                % else:
                                  NO TEST
                                % endif
                              </td>
                              <td>
                                % for key in test_obj.data_used:
                                    ${key}: ${test_obj.data_used[key]}
                                    </br>
                                % endfor
                              </td>
                            </tr>
                        % endfor
                    </table>
                </div>
            % endfor
        </div>
    </body>
</html>
