local awful = require("awful")

--- Tags
--- ~~~~
-- MEADD Changed because I want the last tag (Steam/games
local tag_names = { "1", "2", "3", "4", "5", "6" }
local my_layouts = {
	awful.layout.layouts[2],
	awful.layout.layouts[2],
	awful.layout.layouts[2],
	awful.layout.layouts[2],
	awful.layout.layouts[2],
  awful.layout.layouts[3]
}

screen.connect_signal("request::desktop_decoration", function(s)
	--- Each screen has its own tag table.
	-- MEADD Was layouts[1]
	awful.tag(tag_names, s, my_layouts)
end)
