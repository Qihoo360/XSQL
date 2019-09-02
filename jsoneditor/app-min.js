function QueryParams() {}

function getQueryString(name) {
    var reg = new RegExp("(^|&)" + name + "=([^&]*)(&|$)", "i");
    var r = window.location.search.substr(1).match(reg);
    if (r != null) return JSON.parse(unescape(r[2]));
    return {
            array: [1, 2, 3],
            "boolean": !0,
            "null": null,
            number: 123,
            object: {
                a: "b",
                c: "d",
                e: "f"
            },
            string: "Hello World"
        };
}

function Notify() {
    this.dom = {};
    var a = this;
    jsoneditor.util.addEventListener(document, "keydown", function(b) {
        a.onKeyDown(b)
    })
}

function Splitter(a) {
    if (!a || !a.container) throw new Error("params.container undefined in Splitter constructor");
    var b = this;
    jsoneditor.util.addEventListener(a.container, "mousedown", function(a) {
        b.onMouseDown(a)
    }), this.container = a.container, this.snap = Number(a.snap) || 200, this.width = void 0, this.value = void 0, this.onChange = a.change ? a.change : function() {}, this.params = {}
}
var ajax, FileRetriever, treeEditor, codeEditor, app;
QueryParams.prototype.getQuery = function() {
    var a, b, c, d, e, f, g, h;
    for (a = window.location.search.substring(1), b = a.split("&"), c = {}, d = 0, e = b.length; e > d; d++) f = b[d].split("="), 2 == f.length && (g = decodeURIComponent(f[0]), h = decodeURIComponent(f[1]), c[g] = h);
    return c
}, QueryParams.prototype.setQuery = function(a) {
    var c, d, b = "";
    for (c in a) a.hasOwnProperty(c) && (d = a[c], void 0 != d && (b.length && (b += "&"), b += encodeURIComponent(c), b += "=", b += encodeURIComponent(a[c])));
    window.location.search = b.length ? "#" + b : ""
}, QueryParams.prototype.getValue = function(a) {
    var b = this.getQuery();
    return b[a]
}, QueryParams.prototype.setValue = function(a, b) {
    var c = this.getQuery();
    c[a] = b, this.setQuery(c)
}, ajax = function() {
    function a(a, b, c, d, e) {
        var f, g;
        try {
            if (f = new XMLHttpRequest, f.onreadystatechange = function() {
                    4 == f.readyState && e(f.responseText, f.status)
                }, f.open(a, b, !0), d)
                for (g in d) d.hasOwnProperty(g) && f.setRequestHeader(g, d[g]);
            f.send(c)
        } catch (h) {
            e(h, 0)
        }
    }

    function b(b, c, d) {
        a("GET", b, null, c, d)
    }

    function c(b, c, d, e) {
        a("POST", b, c, d, e)
    }
    return {
        fetch: a,
        get: b,
        post: c
    }
}(), FileRetriever = function(a) {
    a = a || {}, this.options = {
        maxSize: void 0 != a.maxSize ? a.maxSize : 1048576,
        html5: void 0 != a.html5 ? a.html5 : !0
    }, this.timeout = Number(a.timeout) || 3e4, this.headers = {
        Accept: "application/json"
    }, this.scriptUrl = a.scriptUrl || "fileretriever.php", this.notify = a.notify || void 0, this.defaultFilename = "document.json", this.dom = {}
}, FileRetriever.prototype._hide = function(a) {
    a.style.visibility = "hidden", a.style.position = "absolute", a.style.left = "-1000px", a.style.top = "-1000px", a.style.width = "0", a.style.height = "0"
}, FileRetriever.prototype.remove = function() {
    var b, c, a = this.dom;
    for (b in a) a.hasOwnProperty(b) && (c = a[b], c.parentNode && c.parentNode.removeChild(c));
    this.dom = {}
}, FileRetriever.prototype._getFilename = function(a) {
    return a ? a.replace(/^.*[\\\/]/, "") : ""
}, FileRetriever.prototype.setUrl = function(a) {
    this.url = a
}, FileRetriever.prototype.getFilename = function() {
    return this.defaultFilename
}, FileRetriever.prototype.getUrl = function() {
    return this.url
}, FileRetriever.prototype.loadUrl = function(a, b) {
    var c, d, e, f;
    this.setUrl(a), c = void 0, this.notify && (c = this.notify.showNotification("loading url...")), d = this, e = function(a, e) {
        b && (b(a, e), b = void 0), d.notify && c && (d.notify.removeMessage(c), c = void 0)
    }, f = this.scriptUrl, ajax.get(a, d.headers, function(b, c) {
        if (200 == c) e(null, b);
        else {
            var g, h = f + "?url=" + encodeURIComponent(a);
            ajax.get(h, d.headers, function(b, c) {
                200 == c ? e(null, b) : 404 == c ? (console.log('Error: url "' + a + '" not found', c, b), g = new Error('Error: url "' + a + '" not found'), e(g, null)) : (console.log('Error: failed to load url "' + a + '"', c, b), g = new Error('Error: failed to load url "' + a + '"'), e(g, null))
            })
        }
    }), setTimeout(function() {
        e(new Error("Error loading url (time out)"))
    }, this.timeout)
}, FileRetriever.prototype.loadFile = function(a) {
    var g, h, b = void 0,
        c = this,
        d = function() {
            c.notify && !b && (b = c.notify.showNotification("loading file...")), setTimeout(function() {
                e(new Error("Error loading url (time out)"))
            }, c.timeout)
        },
        e = function(d, e) {
            a && (a(d, e), a = void 0), c.notify && b && (c.notify.removeMessage(b), b = void 0)
        },
        f = c.options.html5 && window.File && window.FileReader;
    f ? this.prompt({
        title: "打开文件",
        titleSubmit: "打开",
        description: "请选择一个文件",
        inputType: "file",
        inputName: "file",
        callback: function(a, b) {
            if (a) {
                if (f) {
                    var c = b.files[0],
                        g = new FileReader;
                    g.onload = function(a) {
                        var b = a.target.result;
                        e(null, b)
                    }, g.readAsText(c)
                }
                d()
            }
        }
    }) : (g = "fileretriever-upload-" + Math.round(1e15 * Math.random()), h = document.createElement("iframe"), h.name = g, c._hide(h), h.onload = function() {
        var b, a = h.contentWindow.document.body.innerHTML;
        a && (b = c.scriptUrl + "?id=" + a + "&filename=" + c.getFilename(), ajax.get(b, c.headers, function(a, b) {
            if (200 == b) e(null, a);
            else {
                var d = new Error("Error loading file " + c.getFilename());
                e(d, null)
            }
            h.parentNode === document.body && document.body.removeChild(h)
        }))
    }, document.body.appendChild(h), this.prompt({
        title: "Open file",
        titleSubmit: "打开",
        description: "请选择一个文件",
        inputType: "file",
        inputName: "file",
        formAction: this.scriptUrl,
        formMethod: "POST",
        formTarget: g,
        callback: function(a) {
            a && d()
        }
    }))
}, FileRetriever.prototype.loadUrlDialog = function(a) {
    var b = this;
    this.prompt({
        title: "打开链接",
        titleSubmit: "打开",
        description: "请输入可直接访问的外网地址",
        inputType: "text",
        inputName: "url",
        inputDefault: this.getUrl(),
        callback: function(c) {
            c ? b.loadUrl(c, a) : a()
        }
    })
}, FileRetriever.prototype.prompt = function(a) {
    var f, g, h, i, j, k, l, m, n, o, b = function() {
            o.parentNode && o.parentNode.removeChild(o), e.parentNode && e.parentNode.removeChild(e), jsoneditor.util.removeEventListener(document, "keydown", d)
        },
        c = function() {
            b(), a.callback && a.callback(null)
        },
        d = jsoneditor.util.addEventListener(document, "keydown", function(a) {
            var b = a.which;
            27 == b && (c(), a.preventDefault(), a.stopPropagation())
        }),
        e = document.createElement("div");
    e.className = "fileretriever-overlay", document.body.appendChild(e), f = document.createElement("form"), f.className = "fileretriever-form", f.target = a.formTarget || "", f.action = a.formAction || "", f.method = a.formMethod || "POST", f.enctype = "multipart/form-data", f.encoding = "multipart/form-data", f.onsubmit = function() {
        return i.value ? (setTimeout(function() {
            b()
        }, 0), a.callback && a.callback(i.value, i), void 0 != a.formAction && void 0 != a.formMethod) : (alert("Enter a " + a.inputName + " first..."), !1)
    }, g = document.createElement("div"), g.className = "fileretriever-title", g.appendChild(document.createTextNode(a.title || "Dialog")), f.appendChild(g), a.description && (h = document.createElement("div"), h.className = "fileretriever-description", h.appendChild(document.createTextNode(a.description)), f.appendChild(h)), i = document.createElement("input"), i.className = "fileretriever-field", i.type = a.inputType || "text", i.name = a.inputName || "text", i.value = a.inputDefault || "", j = document.createElement("div"), j.className = "fileretriever-contents", j.appendChild(i), f.appendChild(j), k = document.createElement("input"), k.className = "fileretriever-cancel", k.type = "button", k.value = a.titleCancel || "取消", k.onclick = c, l = document.createElement("input"), l.className = "fileretriever-submit", l.type = "submit", l.value = a.titleSubmit || "Ok", m = document.createElement("div"), m.className = "fileretriever-buttons", m.appendChild(k), m.appendChild(l), f.appendChild(m), n = document.createElement("div"), n.className = "fileretriever-border", n.appendChild(f), o = document.createElement("div"), o.className = "fileretriever-background", o.appendChild(n), o.onclick = function(a) {
        var b = a.target;
        b == o && c()
    }, document.body.appendChild(o), i.focus(), i.select()
}, FileRetriever.prototype.saveFile = function(a, b) {
    var d, e, f, c = void 0;
    this.notify && (c = this.notify.showNotification("saving file...")), d = this, e = function(a) {
        b && (b(a), b = void 0), d.notify && c && (d.notify.removeMessage(c), c = void 0)
    }, f = document.createElement("a"), this.options.html5 && void 0 != f.download && !util.isFirefox() ? (f.style.display = "none", f.href = "data:application/json;charset=utf-8," + encodeURIComponent(a), f.download = this.getFilename(), document.body.appendChild(f), f.click(), document.body.removeChild(f), e()) : a.length < this.options.maxSize ? ajax.post(d.scriptUrl, a, d.headers, function(a, b) {
        if (200 == b) {
            var c = document.createElement("iframe");
            c.src = d.scriptUrl + "?id=" + a + "&filename=" + d.getFilename(), d._hide(c), document.body.appendChild(c), e()
        } else e(new Error("文件保存错误"))
    }) : e(new Error("Maximum allowed file size exceeded (" + this.options.maxSize + " bytes)")), setTimeout(function() {
        e(new Error("Error saving file (time out)"))
    }, this.timeout)
}, Notify.prototype.showNotification = function(a) {
    return this.showMessage({
        type: "notification",
        message: a,
        closeButton: !1
    })
}, Notify.prototype.showError = function(a) {
    return this.showMessage({
        type: "error",
        message: a.message ? "Error: " + a.message : a.toString(),
        closeButton: !0
    })
}, Notify.prototype.showMessage = function(a) {
    var c, d, e, f, g, h, i, j, k, l, m, n, o, b = this.dom.frame;
    return b || (c = 500, d = 5, e = document.body.offsetWidth || window.innerWidth, b = document.createElement("div"), b.style.position = "absolute", b.style.left = (e - c) / 2 + "px", b.style.width = c + "px", b.style.top = d + "px", b.style.zIndex = "999", document.body.appendChild(b), this.dom.frame = b), f = a.type || "notification", g = a.closeButton !== !1, h = document.createElement("div"), h.className = f, h.type = f, h.closeable = g, h.style.position = "relative", b.appendChild(h), i = document.createElement("table"), i.style.width = "100%", h.appendChild(i), j = document.createElement("tbody"), i.appendChild(j), k = document.createElement("tr"), j.appendChild(k), l = document.createElement("td"), l.innerHTML = a.message || "", k.appendChild(l), g && (m = document.createElement("td"), m.style.textAlign = "right", m.style.verticalAlign = "top", k.appendChild(m), n = document.createElement("button"), n.innerHTML = "&times;", n.title = "Close message (ESC)", m.appendChild(n), o = this, n.onclick = function() {
        o.removeMessage(h)
    }), h
}, Notify.prototype.removeMessage = function(a) {
    var c, b = this.dom.frame;
    if (!a && b) {
        for (c = b.firstChild; c && !c.closeable;) c = c.nextSibling;
        c && c.closeable && (a = c)
    }
    a && a.parentNode == b && a.parentNode.removeChild(a), b && 0 == b.childNodes.length && (b.parentNode.removeChild(b), delete this.dom.frame)
}, Notify.prototype.onKeyDown = function(a) {
    var b = a.which;
    27 == b && (this.removeMessage(), a.preventDefault(), a.stopPropagation())
}, Splitter.prototype.onMouseDown = function(a) {
    var b = this,
        c = a.which ? 1 == a.which : 1 == a.button;
    c && (jsoneditor.util.addClassName(this.container, "active"), this.params.mousedown || (this.params.mousedown = !0, this.params.mousemove = jsoneditor.util.addEventListener(document, "mousemove", function(a) {
        b.onMouseMove(a)
    }), this.params.mouseup = jsoneditor.util.addEventListener(document, "mouseup", function(a) {
        b.onMouseUp(a)
    }), this.params.screenX = a.screenX, this.params.changed = !1, this.params.value = this.getValue()), a.preventDefault(), a.stopPropagation())
}, Splitter.prototype.onMouseMove = function(a) {
    if (void 0 != this.width) {
        var b = a.screenX - this.params.screenX,
            c = this.params.value + b / this.width;
        c = this.setValue(c), c != this.params.value && (this.params.changed = !0), this.onChange(c)
    }
    a.preventDefault(), a.stopPropagation()
}, Splitter.prototype.onMouseUp = function(a) {
    if (jsoneditor.util.removeClassName(this.container, "active"), this.params.mousedown) {
        jsoneditor.util.removeEventListener(document, "mousemove", this.params.mousemove), jsoneditor.util.removeEventListener(document, "mouseup", this.params.mouseup), this.params.mousemove = void 0, this.params.mouseup = void 0, this.params.mousedown = !1;
        var b = this.getValue();
        this.params.changed || (0 == b && (b = this.setValue(.2), this.onChange(b)), 1 == b && (b = this.setValue(.8), this.onChange(b)))
    }
    a.preventDefault(), a.stopPropagation()
}, Splitter.prototype.setWidth = function(a) {
    this.width = a
}, Splitter.prototype.setValue = function(a) {
    a = Number(a), void 0 != this.width && this.width > this.snap && (a < this.snap / this.width && (a = 0), a > (this.width - this.snap) / this.width && (a = 1)), this.value = a;
    try {
        localStorage.splitterValue = a
    } catch (b) {
        console && console.log && console.log(b)
    }
    return a
}, Splitter.prototype.getValue = function() {
    var a = this.value;
    if (void 0 == a) try {
        void 0 != localStorage.splitterValue && (a = Number(localStorage.splitterValue), a = this.setValue(a))
    } catch (b) {
        console.log(b)
    }
    return void 0 == a && (a = this.setValue(.5)), a
}, treeEditor = null, codeEditor = null, app = {}, app.CodeToTree = function() {
    try {
        treeEditor.set(codeEditor.get())
    } catch (a) {
        app.notify.showError(app.formatError(a))
    }
}, app.treeToCode = function() {
    try {
        codeEditor.set(treeEditor.get())
    } catch (a) {
        app.notify.showError(app.formatError(a))
    }
}, app.load = function() {
    var a, b, c, d, e, f, g, h, i, j;
    try {
        app.notify = new Notify, app.retriever = new FileRetriever({
            scriptUrl: "fileretriever.php",
            notify: app.notify
        }), a = getQueryString("c"), window.QueryParams && (b = new QueryParams, c = b.getValue("url"), c && (a = {}, app.openUrl(c))), app.lastChanged = void 0, d = document.getElementById("codeEditor"), codeEditor = new jsoneditor.JSONEditor(d, {
            mode: "code",
            change: function() {
                app.lastChanged = codeEditor
            },
            error: function(a) {
                app.notify.showError(app.formatError(a))
            }
        }), codeEditor.set(a), d = document.getElementById("treeEditor"), treeEditor = new jsoneditor.JSONEditor(d, {
            mode: "tree",
            change: function() {
                app.lastChanged = treeEditor
            },
            error: function(a) {
                app.notify.showError(app.formatError(a))
            }
        }), treeEditor.set(a), app.splitter = new Splitter({
            container: document.getElementById("drag"),
            change: function() {
                app.resize()
            }
        }), e = document.getElementById("toTree"), e.onclick = function() {
            this.focus(), app.CodeToTree()
        }, f = document.getElementById("toCode"), f.onclick = function() {
            this.focus(), app.treeToCode()
        }, jsoneditor.util.addEventListener(window, "resize", app.resize), g = document.getElementById("clear"), g.onclick = app.clearFile, h = document.getElementById("menuOpenFile"), h.onclick = function(a) {
            app.openFile(), a.stopPropagation(), a.preventDefault()
        }, i = document.getElementById("menuOpenUrl"), i.onclick = function(a) {
            app.openUrl(), a.stopPropagation(), a.preventDefault()
        }, j = document.getElementById("save"), j.onclick = app.saveFile, codeEditor.focus(), document.body.spellcheck = !1
    } catch (k) {
        try {
            app.notify.showError(k)
        } catch (l) {
            console && console.log && console.log(k), alert(k)
        }
    }
}, app.openCallback = function(a, b) {
    if (a) app.notify.showError(a);
    else if (null != b) {
        codeEditor.setText(b);
        try {
            var c = jsoneditor.util.parse(b);
            treeEditor.set(c)
        } catch (a) {
            treeEditor.set({}), app.notify.showError(app.formatError(a))
        }
    }
}, app.openFile = function() {
    app.retriever.loadFile(app.openCallback)
}, app.openUrl = function(a) {
    a ? app.retriever.loadUrl(a, app.openCallback) : app.retriever.loadUrlDialog(app.openCallback)
}, app.saveFile = function() {
    app.lastChanged == treeEditor && app.treeToCode(), app.lastChanged = void 0;
    var a = codeEditor.getText();
    app.retriever.saveFile(a, function(a) {
        a && app.notify.showError(a)
    })
}, app.formatError = function(a) {
    var b = '<pre class="error">' + a.toString() + "</pre>";
    return b
}, app.clearFile = function() {
    var a = {};
    codeEditor.set(a), treeEditor.set(a)
}, app.resize = function() {
    var k, l, m, n, o, p, q, a = document.getElementById("menu"),
        b = document.getElementById("treeEditor"),
        c = document.getElementById("codeEditor"),
        d = document.getElementById("splitter"),
        e = document.getElementById("buttons"),
        f = document.getElementById("drag"),
        g = document.getElementById("ad"),
        h = 15,
        i = window.innerWidth || document.body.offsetWidth || document.documentElement.offsetWidth,
        j = g ? g.clientWidth : 0;
    j && (i -= j + h), app.splitter && (app.splitter.setWidth(i), k = app.splitter.getValue(), l = k > 0, m = 1 > k, n = l && m, e.style.display = n ? "" : "none", p = d.clientWidth, l ? m ? (o = i * k - p / 2, q = 8 == jsoneditor.util.getInternetExplorerVersion(), f.innerHTML = q ? "|" : "&#8942;", f.title = "向左或向右拖动改变面板的宽度") : (o = i * k - p, f.innerHTML = "&lsaquo;", f.title = "左拖动显示节点编辑器") : (o = 0, f.innerHTML = "&rsaquo;", f.title = "右拖动显示代码编辑器"), c.style.display = 0 == k ? "none" : "", c.style.width = Math.max(Math.round(o), 0) + "px", codeEditor.resize(), f.style.height = d.clientHeight - e.clientHeight - 2 * h - (n ? h : 0) + "px", f.style.lineHeight = f.style.height, b.style.display = 1 == k ? "none" : "", b.style.left = Math.round(o + p) + "px", b.style.width = Math.max(Math.round(i - o - p - 2), 0) + "px"), a && (a.style.right = j ? h + (j + h) + "px" : h + "px")
};