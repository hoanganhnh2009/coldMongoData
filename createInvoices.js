var request = require("request");
var rp = require("request-promise");
let defaultState = {
  name: "",
  telephone: "",
  name_receiver: "",
  invoicestatus: "",
  city: "",
  district: "",
  ward: "",
  address: "",
  note: "",
  hinhthucvc: "",
  discount_percent: "0",
  discount_amount: "0",
  s_h_amount: "0",
  ghichu1: "",
  ghichu2: "",
  notevanchuyen: "",
  giamgia: "0",
  phuthu: "0",
  phivanchuyen: "0",
  adjustment: "0.000",
  taxtype: "individual",
  deposits: 0,
  subtotal: 0,
  taxtotal_invoice: "0",
  total: 0,
  list_clientgroup: "",
  accountid: 0,
  productstoreid: 0,
  list_pageitem: "",
  listProduct: []
};
function createInvoices(item) {
  const headers = {
    "Content-Type": "application/json"
  };
  let url =
    "https://new.abit.vn/invoices/create?dynamic_key=MTU2Mzk4ODU5Mnw2MDZjMWE1OTc3NTdkYjc3NTkxMjJkODAxMTNjMDIyMnwxMzF8Mnxjb25uZWN0XzIzNA==";
  let formData = {
    ...defaultState,
    // ...item
    name: item.username,
    name_receiver: item.username,
    telephone: item.number_phone.join(",")
  };
  let body = JSON.stringify([formData]);
  return new Promise((resolve, reject) => {
    request(
      {
        headers: headers,
        uri: url,
        body: body,
        method: "POST"
      },
      function(error, response, body) {
        if (error) {
          console.log(error);
          return reject({ error });
        } else {
          // try {
          if (body) {
            body = JSON.parse(body);
            console.log("TCL: createInvoices -> Mã đơn: ", body.invoice_no);
            if (body.status === "success") {
              // sendNotification(body, formData.name);
              return resolve({ success: true, payload: body });
            }
            return reject({
              error: body
            });
          }else{
            return reject({
              error: {
                code:503
              }
            });
          }
          /*   body = JSON.parse(body);
            if (body.status === "success") {
              // sendNotification(body, formData.name);
              return resolve({ success: true, payload: body });
            }
            return reject({
              error: {
                body
              }
            });
          } catch (error) {
            console.log("TCL: createInvoices -> error", error)
            return reject({
              error: {
                message: "Server Error",
                code: 501
              }
            }); */
        }
        // }
      }
    );
  });
}

function sendNotification(response, name) {
  let body = {
    to: "/topics/shop_1",
    notification: {
      title: "Đơn hàng từ " + name,
      body: "Xem đơn " + response.invoice_no + " trên Abitstore.vn",
      sound: "bell.mp3",
      show_in_foreground: true,
      badge: 1,
      ticker: "My Notification Ticker",
      click_action: "vn.nitco.Abitstore",
      icon: "ic_launcher"
    },
    data: {
      targetScreen: "_billDetail",
      id: response.invoiceid
    },
    priority: "high"
  };
  const headers = {
    "Content-Type": "application/json",
    Authorization:
      "key=AAAAFKtxeJk:APA91bF12mk18OzRw57TP1fcCrU96E9IpiBQQzTP8GK_sI1H3quzCYZ_Muh1c9xNfAtHi7uGs-w5oYF6NK1mSFPcqh5aAeSOrnNdTyrgDZH2n_1UsK32vuZfGp2NNRRkJPO2fqPkL1tt"
  };
  let url = "https://fcm.googleapis.com/fcm/send";
  body = JSON.stringify(body);
  request(
    {
      headers: headers,
      uri: url,
      body: body,
      method: "POST"
    },
    function(error, response, body) {
      console.log("TCL: sendNotification -> body", body);
    }
  );
}
// sendNotification({"status":"success","code":"202","message":"Tạo đơn hàng thành công","invoiceid":"13996259","invoice_no":"EG13811982-64"},"Thành")
// createInvoices();
module.exports = createInvoices;
