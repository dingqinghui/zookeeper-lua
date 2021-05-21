



a = 0
function func()
    debug.setupvalue( func, 1, {b = 1,print = print} )
    print(b,a)
end 


func()