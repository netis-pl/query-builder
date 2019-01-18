<?php

namespace netis\queryBuilder;

interface AttributeMappingInterface
{
    public function attributeMap($attribute);

    public function getMappingByAttribute($attribute, $value);
}
