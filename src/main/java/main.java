/*
 * The MIT License
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */
import com.influxdb.v3.client.InfluxDBClient;
import com.influxdb.v3.client.PointValues;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class main {
    public static void main(String[] args) {

        String host = "https://us-east-1-1.aws.cloud2.influxdata.com";
        char[] token = "xWh3VQCb3pMJPw7T2lnEwFLXO-pb4OWzfNN76UTpmRKtlg83yJlz6maLC3AL0B6M6gMWWZY2QApzSdEeEopWlQ==".toCharArray();
        String database = "admin";

//        List<PointValues> arrayList = new ArrayList<>();
        try (InfluxDBClient client = InfluxDBClient.getInstance(host, token, database)) {
//            arrayList = client.queryPoints("SELECT * FROM host2")
//                              .collect(Collectors.toList());
//            System.out.println("arrayList = " + arrayList);
            Stream<Object[]> query = client.query("SELECT * FROM host2");
            query.forEach(row -> {
                System.out.println("row = " + Arrays.toString(row));
            });
            client.close();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
