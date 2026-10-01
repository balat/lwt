
let () = Test.run "lwt_direct" (Test_lwt_direct.suites @ Test_await_anywhere.suites) ;;
